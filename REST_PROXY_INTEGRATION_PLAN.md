# Integrating the embedded Kafka REST proxy into Strimzi

Branch: `rest-api`
Companion Kafka fork: https://github.com/Raikion201/kafka (branch `rest-proxy`)

## Goal

Make a Kafka cluster deployed by Strimzi (via the `Kafka` CRD) expose the
embedded HTTP REST proxy that lives inside the broker JVM — the one built
in the companion fork. End state: a user writes

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    version: 4.4.0-rest-proxy
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: http
        port: 8080
        type: http                       # NEW
        tls: false
      - name: https
        port: 8443
        type: https                      # NEW
        tls: true
        configuration:
          brokerCertChainAndKey:
            secretName: my-http-cert
            certificate: tls.crt
            key: tls.key
  ...
```

…and Strimzi brings up a cluster where:

1. Every broker JVM runs the REST proxy natively on ports 8080 and 8443.
2. `Services` / `Routes` / `Ingresses` expose the HTTP listener(s) to the
   outside world the same way Strimzi already does for TCP listeners.
3. The HTTPS listener shares the broker's `SslFactory`-based cert pipeline —
   rotation via `kafka-configs.sh --alter` continues to work.
4. Operator-level status / metrics / probes remain functional.

There is no separate `KafkaBridge` pod in this design. The REST API is
co-located with the broker, not sidecared next to it.

---

## Why this is non-trivial

Strimzi was not written with "a listener that isn't a Kafka
`SecurityProtocol`" in mind. Every existing listener type ends up both in
`listeners=` **and** in `listener.security.protocol.map`, and the operator
assumes a 1:1 mapping. The REST proxy deliberately sidesteps
`listener.security.protocol.map` — it's the whole reason
`KafkaConfig.httpListeners` filters HTTP/HTTPS out before
`listenerListToEndPoints` runs. Teaching Strimzi about a listener type
that contributes to `listeners=` but **not** to `listener.security.protocol.map`
is the structurally interesting part of this work.

Key surfaces the change has to touch:

| Concern | Where it lives today | What has to change |
|---|---|---|
| Listener type enum | `api/.../listener/KafkaListenerType.java` | Add `HTTP`, `HTTPS`. |
| CRD JSON schema | `api/.../crd/*.yaml` (generated from annotations) | Regenerate after enum change; extend JSON-schema validation. |
| `listeners=` / `listener.security.protocol.map` renderer | `cluster-operator/.../KafkaBrokerConfigurationBuilder.java` | Append HTTP/HTTPS to `listeners=` only; skip the `protocol.map` entry; don't wire SSL through the "listener.name.http.ssl.*" shape the same way. |
| Listener buckets in utils | `ListenersUtils.java` | Add `httpListeners(...)`, `httpsListeners(...)`; teach the NodePort / LoadBalancer / Ingress helpers to treat them appropriately. |
| Validation rules | `ListenersValidator.java` | Port uniqueness, TLS-required-on-HTTPS, auth-rules-not-applicable-to-HTTP. |
| Service / Route / Ingress generators | `KafkaCluster.java` + `cluster-operator/.../KafkaAssemblyOperator` | HTTP listeners need a `Service` and optional `Ingress`/`Route`; must not generate `KafkaListenerClientAuth` stuff. |
| Probe ports | `cluster-operator/.../KafkaCluster.generateStatefulSet` | HTTP listener can serve as a readiness probe endpoint. |
| Broker image | `docker-images/kafka-based/kafka/Dockerfile` + `kafka-versions.yaml` | Pull a Kafka tarball built from the fork (includes `:http` and `:http-api` jars). |
| Cert rotation plumbing | `cluster-operator/.../CertUtils` etc. | When Strimzi issues/rotates the HTTPS listener cert, the broker's `DynamicBrokerConfig.alter` path (via the REST proxy's `KafkaSslContextFactory`) handles it without restart — no Strimzi code needed here, just verification. |
| Metrics | `cluster-operator/.../KafkaCluster.generatePodTemplateSpec` | Jetty's thread pool / connection metrics need to be surfaced via JMX exporter if we want consistency with the rest of Kafka metrics. |

---

## Phase 1 — Ship a Kafka distribution that contains the REST proxy

**Goal:** produce a Kafka tarball that Strimzi's image-build pipeline can
consume exactly like an upstream release.

**Steps:**

1. In the companion Kafka fork, run `./gradlew releaseTarGz -PscalaVersion=2.13`.
   The artifact is `core/build/distributions/kafka_2.13-<ver>.tgz`, structured
   identically to upstream releases except the `libs/` folder includes the new
   `kafka-http-<ver>.jar` and `kafka-http-api-<ver>.jar`.
2. Host the tarball somewhere addressable (GitHub Release, internal Nexus,
   S3 bucket with presigned URL). Record the SHA-512.
3. Add an entry to `kafka-versions.yaml`:

   ```yaml
   - version: 4.4.0-rest-proxy
     metadata: 4.4
     url: https://github.com/Raikion201/kafka/releases/download/rest-proxy-0.1/kafka_2.13-4.4.0-rest-proxy.tgz
     checksum: <SHA-512>
     third-party-libs: 4.4.x
     supported: true
     default: false
   ```
4. Verify `make java_build` still succeeds; the operator's `KafkaVersion`
   loader picks up the new entry automatically.

**Risk:** keeping the fork up-to-date with upstream. Mitigation: land the
change upstream (KIP) so Strimzi picks it up via a normal version bump.
Until then, the fork needs periodic rebase on Apache Kafka `trunk`.

---

## Phase 2 — Extend `KafkaListenerType`

**File:** `api/src/main/java/io/strimzi/api/kafka/model/kafka/listener/KafkaListenerType.java`

Add two enum values and their string mappings:

```java
public enum KafkaListenerType {
    INTERNAL,
    ROUTE,
    LOADBALANCER,
    NODEPORT,
    INGRESS,
    CLUSTER_IP,
    HTTP,       // NEW
    HTTPS;      // NEW
    ...
}
```

Also extend `forValue` / `toValue`. That alone makes the CRD validator
accept `type: http` / `type: https` in `Kafka.spec.kafka.listeners`.

**Regenerate CRDs:** `make crd_install` (or the specific Maven goal Strimzi
uses — check `crd-generator/pom.xml`). Commit the updated YAML under
`packaging/install/cluster-operator/` and `helm-charts/`.

**Compatibility:** existing `Kafka` resources are unaffected; the enum is
additive.

---

## Phase 3 — Operator: emit the right `server.properties`

**File:** `cluster-operator/src/main/java/io/strimzi/operator/cluster/model/KafkaBrokerConfigurationBuilder.java`

Today's loop (around lines 278–301) does:

```java
for (GenericKafkaListener listener : kafkaListeners) {
    ...
    listeners.add(listenerName + "://0.0.0.0:" + port);
    advertisedListeners.add(...);
    configureAuthentication(listenerName, securityProtocol, listener.isTls(), ...);
    configureListener(listenerName, listener.getConfiguration());
    if (listener.isTls()) configureTlsOnListener(listenerName, customServerCert);
}
```

For HTTP/HTTPS listeners this has to be different. The embedded REST proxy
expects exactly `HTTP://host:port` / `HTTPS://host:port` literals (the
regex that splits them off in `KafkaConfig.httpListeners` is
`^(?i)(HTTPS?)://...$` — it takes the protocol word as the listener name).
It must not appear in `listener.security.protocol.map`.

Concrete changes:

```java
for (GenericKafkaListener listener : kafkaListeners) {
    if (listener.getType() == KafkaListenerType.HTTP) {
        listeners.add("HTTP://0.0.0.0:" + listener.getPort());
        // No entry in securityProtocol; no configureAuthentication; no configureTls.
        continue;
    }
    if (listener.getType() == KafkaListenerType.HTTPS) {
        listeners.add("HTTPS://0.0.0.0:" + listener.getPort());
        configureTlsOnListener("HTTPS", listener.getConfiguration().getBrokerCertChainAndKey());
        // No entry in securityProtocol; HTTPS is not a Kafka SecurityProtocol.
        continue;
    }
    // ... existing TCP-listener path unchanged ...
}
```

Then emit the REST-proxy-specific keys from the listener's
`configuration` block:

- `http.rest.basic.credentials` → from a `SecretKeySelector` in the listener
  configuration (new field). Same pattern the bridge uses for its HTTP auth.
- `http.rest.executor.threads` → from a new `configuration.executorThreads`
  field (default 8).
- `http.rest.swagger-ui.enabled` → from a new `configuration.swaggerUiEnabled`
  field (default false, matching the broker default).

These are all `spec.kafka.config`-style knobs; Strimzi allows that surface
but validates it against an allow-list — `config-model-generator/` will need
an entry so they pass validation.

**Tests:** `cluster-operator/src/test/.../KafkaBrokerConfigurationBuilderTest.java`.
Add cases that assert the rendered `server.properties` contains
`listeners=CONTROLPLANE-9090://...,REPLICATION-9091://...,PLAIN-9092://...,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443`
and **does not** contain `HTTP:PLAINTEXT` or `HTTPS:SSL` entries in
`listener.security.protocol.map`.

---

## Phase 4 — `ListenersUtils` + `ListenersValidator`

**File:** `cluster-operator/.../ListenersUtils.java`

Add bucketing helpers:

```java
public static List<GenericKafkaListener> httpListeners(List<GenericKafkaListener> listeners) {
    return listenersByType(listeners, KafkaListenerType.HTTP);
}
public static List<GenericKafkaListener> httpsListeners(List<GenericKafkaListener> listeners) {
    return listenersByType(listeners, KafkaListenerType.HTTPS);
}
public static boolean hasAnyHttpListener(List<GenericKafkaListener> listeners) {
    return !httpListeners(listeners).isEmpty() || !httpsListeners(listeners).isEmpty();
}
```

Decide where in the existing NodePort / LoadBalancer / Ingress bucketing
helpers HTTP/HTTPS should appear. Most conservative answer: they don't —
they get their own external-exposure path (Phase 5).

**File:** `ListenersValidator.java`

Add rules:

- HTTP listener must have `tls: false`; HTTPS must have `tls: true`.
- Neither type supports `authentication: { type: tls | scramSha512 | oauth }` —
  the REST proxy uses HTTP Basic, wired separately.
- Port uniqueness still applies (already generic).
- HTTPS listener must have either a global `ssl.keystore.*` or a
  `configuration.brokerCertChainAndKey` — otherwise the broker's
  `SslFactory.configure` will fail at startup and the error will only
  surface in the Pod logs, not in a `Kafka` status condition.

Each rule is a new `if` block in the existing `validate(Kafka kafka)` method.

**Tests:** `ListenersValidatorTest.java` — add positive and negative cases
for each rule. Strimzi's test style is table-driven `@ParameterizedTest`.

---

## Phase 5 — Service / Ingress / Route generation

**File:** `cluster-operator/.../KafkaCluster.java`

Strimzi generates a `Service` per external listener (`kafka.io/...-bootstrap`
and per-broker services). For HTTP/HTTPS listeners we want:

1. A headless per-broker service (for direct broker access from inside
   the cluster), same shape as the PLAINTEXT internal listener.
2. An external bootstrap service — type depends on the listener's
   `configuration.bootstrap`: `ClusterIP` by default; if the user adds
   `configuration: { bootstrap: { type: loadbalancer } }` we create an
   `ExternalName` / `LoadBalancer` Service accordingly.

Concretely: extend the existing `generateExternalServices` /
`generateInternalService` loops to include HTTP/HTTPS listeners. Most of
the code is reusable — the change is just "loop over HTTP/HTTPS too."

For TLS termination: **do not terminate TLS at the Service**. The HTTPS
listener already terminates it inside the broker JVM via `SslFactory`. The
Service must be a pure TCP passthrough.

**Ingress** (`configuration.ingressClassName`): Strimzi today supports
`type: ingress` with SNI routing for TLS-passthrough. For HTTPS-via-REST,
SNI routing works the same — the Ingress controller sends the HTTPS bytes
unchanged to the backend; the broker does TLS termination. For plain HTTP,
an Ingress with HTTP routing rules is fine.

**Routes** (OpenShift): same story, different CRD (`Route`).

The diff here is moderate — every generator method needs a switch on
`type == HTTP || type == HTTPS`. Bulk of the logic is re-use, not new code.

---

## Phase 6 — TLS integration with Strimzi's cert manager

Strimzi today, for `tls: true` internal/cluster-ip/etc. listeners, issues
a broker cert from the cluster CA and mounts it as a Secret. The broker is
told `ssl.keystore.location=/tmp/kafka/cluster.keystore.p12` and
`ssl.keystore.password=<generated>`.

For HTTPS-REST listeners, two modes:

- **Automatic cert** — reuse the existing broker cert (same keystore
  already mounted). Nothing new to do. The `listener.name.https.ssl.*`
  entries the REST proxy expects will fall back to the global `ssl.*`
  since Strimzi only sets the global.
- **User-supplied cert** — `configuration.brokerCertChainAndKey` points at
  a user-created Secret. Strimzi mounts that Secret as a separate keystore
  and emits `listener.name.https.ssl.keystore.location=<path>`, exactly
  matching what the companion fork's `HttpsRestProxyIntegrationTest`
  already verifies.

No new cert-manager logic — the existing per-listener override flow from
Strimzi handles both cases once the listener type is recognised.

**Rotation:** when Strimzi rolls the broker cert (e.g. CA renewal),
`DynamicBrokerConfig` on the broker side fires `reconfigure` on
`KafkaSslContextFactory`, which hot-swaps the engine — no pod restart
required. Phase 8 should include an integration test that verifies this
end-to-end.

---

## Phase 7 — Auth config (`http.rest.basic.credentials`)

Add a listener-configuration field that accepts a Secret reference:

```yaml
listeners:
  - name: http
    port: 8080
    type: http
    tls: false
    configuration:
      basicCredentials:
        secretName: rest-users
        key: users.txt           # value must be user:pass,user:pass,...
```

**Operator plumbing:**

1. At reconciliation time, read the Secret, decode, pass the raw string
   to `KafkaBrokerConfigurationBuilder.configureHttpRestAuth(...)`.
2. Emit `http.rest.basic.credentials=<value>` into `server.properties` —
   a `PASSWORD`-typed config so it's redacted in logs.
3. When the Secret changes, Strimzi's existing Secret-watch triggers a
   rolling update. Acceptable for a first cut; future work: plumb this
   through the dynamic-config path too (the REST proxy's basic-creds are
   read once at startup today — making them reconfigurable is a Kafka-side
   change, not Strimzi's).

**Tests:** Secret-missing should fail fast with a `Kafka` status
condition; Secret-malformed (no `user:pass` shape) must be caught by the
existing eager `BasicCredentialsValidator` when the broker boots, and the
Pod's init failure should surface in the operator status.

---

## Phase 8 — Testing

### Unit

- `KafkaBrokerConfigurationBuilderTest` — new cases for each of:
  - HTTP listener only
  - HTTPS listener only
  - Mixed (internal + HTTP + HTTPS)
  - HTTPS with custom cert
- `ListenersValidatorTest` — a row per new rule.
- `ListenersUtilsTest` — bucket helpers.

### System (systemtest/)

Strimzi's `systemtest/` module runs real Kubernetes clusters. Add:

- `HttpListenerST` — spin up a cluster with HTTP listener, `curl` it from
  a client pod, assert 200/partition/offset response.
- `HttpsListenerST` — same with TLS, including CA trust.
- `HttpsCertRotationST` — alter the Secret backing the HTTPS cert, confirm
  the listener serves the new cert without restart (this is the test the
  companion fork writes at unit level against `SslFactory.reconfigure`;
  here it's the operator+broker version).
- `RestProxyFailoverST` — kill the broker pod, verify the HTTP listener
  comes back up after `StatefulSet` reschedule.

### Manual

Document a minimal `examples/kafka/kafka-with-rest.yaml` under
`examples/kafka/` with a `curl` walkthrough in the README.

---

## Phase 9 — Docs

- `documentation/modules/assembly-kafka-listeners.adoc` — new subsection
  explaining HTTP/HTTPS listener type, config schema, and the trade-off
  vs `KafkaBridge`.
- `CHANGELOG.md` — user-facing entry.
- `KAFKA_VERSION_SUPPORT.md` — note that the REST proxy requires the
  forked Kafka build until upstream merges it.
- `examples/kafka/kafka-rest-proxy.yaml` — minimal working example.

---

## Rollout phasing

| Milestone | Deliverable | Estimated effort |
|---|---|---|
| M1 | Phase 1 (forked tarball published, `kafka-versions.yaml` entry), Phase 2 (enum + CRD regen) | 1 week |
| M2 | Phase 3 + Phase 4 (config render + validators) with unit tests | 2 weeks |
| M3 | Phase 5 (Service/Ingress generators) + internal smoke test | 2 weeks |
| M4 | Phase 6 + Phase 7 (TLS + Basic Auth Secret plumbing) | 1 week |
| M5 | Phase 8 systemtests green on a real k8s cluster | 2 weeks |
| M6 | Phase 9 docs, examples, CHANGELOG; internal RC | 1 week |

**Critical path:** Phase 3 is the highest-complexity block — touching
`KafkaBrokerConfigurationBuilder` without breaking the existing listeners
is load-bearing, and the tests there are the finest-grained safety net.

---

## Risks

1. **Upstream divergence.** Every rebase of the Kafka fork onto Apache
   `trunk` has to keep `:http` and `:http-api` compiling against moving
   Kafka internals. Mitigation: land the REST proxy as a KIP upstream.
   Until then, pin the fork to specific Apache release tags.
2. **Strimzi API stability.** Adding enum values to `KafkaListenerType` is
   backwards-compatible, but changing `KafkaBrokerConfigurationBuilder`
   has to remain compatible with brokers that don't have the REST proxy
   (i.e. unforked Kafka versions listed in `kafka-versions.yaml`).
   Mitigation: the HTTP/HTTPS branches in the config builder should be
   behind a `KafkaVersion.supportsRestProxy()` check — if the operator is
   pointed at an old Kafka version, HTTP/HTTPS listeners become a
   validation error ("this Kafka version does not support REST listeners").
3. **Upgrade path.** A cluster running today with no HTTP listener must
   upgrade cleanly to an operator that knows about the new enum values.
   Covered by the additive nature of the enum, but needs an upgrade
   systemtest.
4. **Image size.** Adding Jetty + Jersey to every broker image grows the
   image by ~20 MB. Acceptable.
5. **Kubernetes policy surprises.** Many cluster admins lock down egress
   Services with NetworkPolicies. The HTTP listener will need an explicit
   policy admission — document it.

---

## Non-goals for this iteration

- Consumer / offset-commit endpoints. The embedded REST proxy is
  single-produce by design. Users who need full HTTP consumer surface
  should use `KafkaBridge` alongside — the two can coexist on the same
  `Kafka` CR.
- Rate limiting / quota per HTTP user. Broker-level quotas apply
  (the produce goes through `ReplicaManager.appendRecords`), but no
  REST-specific tier.
- OAuth / mTLS for REST auth. v1 ships Basic Auth only; richer auth is a
  follow-up once upstream KIP lands.

---

## First commit proposed on this branch

Phase 1 + Phase 2 together — the smallest PR that proves the end-to-end
wiring works:

- `kafka-versions.yaml`: add `4.4.0-rest-proxy` entry (non-default).
- `api/.../KafkaListenerType.java`: add `HTTP`, `HTTPS`.
- Regenerate CRDs under `packaging/install/cluster-operator/`.
- A single new test: `KafkaListenerTypeTest.httpAndHttpsValuesRoundTrip`.

This gets the enum into the CRD, unblocks downstream phases in parallel,
and does not yet allow a user to successfully deploy an HTTP listener —
that requires Phase 3. Reviewers get something small and obvious first.
