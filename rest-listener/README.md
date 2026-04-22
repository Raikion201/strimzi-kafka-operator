# `rest-listener` — Strimzi support for the embedded Kafka REST proxy

End-to-end overview of the Strimzi-side work done on branch `rest-api`
to integrate the embedded HTTP REST proxy from the companion Kafka fork
(https://github.com/Raikion201/kafka, branch `rest-proxy`).

The goal is: a user writes a single `Kafka` CR, applies it, and Strimzi
provisions a `Service type: LoadBalancer` reachable from outside the
cluster, so a `curl http://<lb>:8080/v1/topics/foo` from the user's
laptop round-trips through `POST /v1/topics/{name}` and returns
`{"partition": N, "offset": N}` from the embedded broker-side REST proxy.

---

## What this module introduces

Two new values for `spec.kafka.listeners[].type` in the `Kafka` CRD:

| `type` | Purpose |
|---|---|
| `http`  | Plain HTTP REST proxy listener — serves `POST /v1/topics/{name}` |
| `https` | Same surface, wrapped in TLS via Kafka's `SslFactory` |

They only work against a Kafka build that has the embedded REST proxy
compiled in. See [`BUILDING_THE_KAFKA_IMAGE.md`](./BUILDING_THE_KAFKA_IMAGE.md)
for the recipe; also see `kafka-versions.yaml` for the
`4.4.0-rest-proxy` entry that declares `supports-rest-proxy: true`.

## Minimal Kafka CR

```yaml
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    version: 4.4.0-rest-proxy            # must match a kafka-versions.yaml entry
                                          # with `supports-rest-proxy: true`
    listeners:
      - name: rest
        port: 8080
        type: http
        tls: false
    config:
      http.rest.basic.credentials: alice:s3cret
      http.rest.executor.threads: 8
```

A full copy-paste CR, including `KafkaNodePool` resources, is at
[`../examples/kafka/kafka-rest-proxy.yaml`](../examples/kafka/kafka-rest-proxy.yaml).

---

## What the operator does with a `type: http` listener

1. **`server.properties`.** Appends `HTTP://0.0.0.0:<port>` to
   `listeners=`. **Does not** append to `listener.security.protocol.map`
   or `advertised.listeners=` — the embedded proxy sits outside
   Kafka's security-protocol machinery, and putting `HTTP://` into
   those would cause `listenerListToEndPoints` to fail at broker
   startup.
2. **Kubernetes Service.** Creates `<cluster>-kafka-<listenerName>-bootstrap`
   with `spec.type: LoadBalancer` by default, pointing at the broker
   pods on the listener's port.
3. **No per-broker Services.** REST requests work against any broker
   (stateless with respect to partition ownership), so the bootstrap
   Service that round-robins across broker pods is sufficient. Per-broker
   Services are skipped.
4. **Admission-time gate.** If `spec.kafka.version` does not
   declare `supports-rest-proxy: true`, the CR is rejected before
   the operator tries to apply anything.

### HTTPS listener

Same as above plus:

- `tls: true` (enforced by the validator; plain HTTP listeners must have
  `tls: false`).
- Optional custom cert via `configuration.brokerCertChainAndKey` —
  reuses Strimzi's existing per-listener TLS plumbing; the generated
  `listener.name.https.ssl.keystore.*` keys match what the broker-side
  `KafkaSslContextFactory` looks up via
  `valuesWithPrefixOverride(listener.name.https.)`.

---

## Validation rules

Enforced at CR admission time by `HttpListenerValidator`:

- HTTPS listener must have `tls: true`.
- HTTP listener must have `tls: false`.
- Listener-level authentication (`authentication: { type: tls | scramSha512 | oauth }`)
  is rejected — REST auth is HTTP Basic via `http.rest.basic.credentials`.
- Kafka version must have `supports-rest-proxy: true`.

---

## Why a separate module?

Discipline: no new files were added inside `cluster-operator/`, `api/`, or
any other pre-existing Strimzi module. Every piece of new HTTP-specific
logic lives in `rest-listener/`. The existing modules receive only
narrow dispatch calls:

| Existing file | Delta |
|---|---|
| `api/.../KafkaListenerType.java` | 2 new enum values (`HTTP`, `HTTPS`) + 8 `forValue` / `toValue` case lines |
| `cluster-operator/.../KafkaBrokerConfigurationBuilder.java` | One `if (isHttpOrHttps) { configure(listeners, listener); ... continue; }` at the top of the listener loop |
| `cluster-operator/.../KafkaCluster.java` | Stream filter for per-broker services + one version-gate call in `fromCrd` |
| `cluster-operator/.../ListenersUtils.java` | Three-line dispatch before the existing `serviceType` switch |
| `cluster-operator/.../ListenersValidator.java` | One `errors.addAll(HttpListenerValidator.validate(listener))` |
| `cluster-operator/.../KafkaVersion.java` | New `supportsRestProxy` field + getter + constructor arg |
| `cluster-operator/.../KRaftVersionChangeCreator.java` | 2 constructor call-site updates |
| `.checkstyle/import-control.xml` | 1 allow entry for `io.strimzi.operator.cluster.rest` |
| `pom.xml` | `<module>rest-listener</module>` + internal dep declaration |
| `cluster-operator/pom.xml` | 1 `<dependency>rest-listener</dependency>` entry |
| `kafka-versions.yaml` | New `4.4.0-rest-proxy` entry |

Drop `rest-listener/` and the operator falls back to its pre-REST
behaviour with zero changes elsewhere. Existing listener types
(`internal`, `route`, `loadbalancer`, `nodeport`, `ingress`, `cluster-ip`)
are byte-for-byte unchanged.

---

## Files in this module

| Path | Role |
|---|---|
| `src/main/java/.../HttpListenerTypeSupport.java` | Predicates (`isHttp`, `isHttps`, `isHttpOrHttps`) + wire-name derivation (`wireName`) |
| `src/main/java/.../HttpListenerConfigurer.java`  | Renders `HTTP://0.0.0.0:<port>` into the operator's `listeners=` accumulator |
| `src/main/java/.../HttpListenerServices.java`    | `serviceType()` → `"LoadBalancer"`; `skipPerPodService()` → `true` |
| `src/main/java/.../HttpListenerValidator.java`   | CR-level rule set + version-support gate |
| `src/test/java/.../HttpListenerTypeSupportTest.java` | 6 unit tests |
| `src/test/java/.../HttpListenerConfigurerTest.java`  | 4 unit tests |
| `src/test/java/.../HttpListenerServicesTest.java`    | 6 unit tests |
| `src/test/java/.../HttpListenerValidatorTest.java`   | 10 unit tests (6 rules + 4 version-gate) |
| `BUILDING_THE_KAFKA_IMAGE.md`                    | Step-by-step recipe to produce the forked Kafka tarball and the Strimzi-compatible Kafka image |

Plus one systemtest co-located with Strimzi's other listener STs:

- `../systemtest/.../kafka/listeners/HttpListenerST.java` — end-to-end
  test that deploys a Kafka CR with a `type: http` listener on a real
  Kubernetes cluster, waits for the LoadBalancer, and `curl`s the REST
  endpoint. This is the automated version of the stakeholder demo.

---

## Test status

| Suite | Tests | Status |
|---|---|---|
| `rest-listener` unit tests | 26 | ✅ all pass (`mvn -pl rest-listener test`) |
| `cluster-operator` regression (5 classes touched) | 172 | ✅ all pass |
| `systemtest` | 1 | Compiles clean; requires a real k8s cluster + published Kafka image to execute |

---

## End-to-end demo

From the Kafka companion repo there's a one-shot script at
`../kafka/demo-strimzi-rest-proxy.sh` that:

1. Builds the Kafka tarball from the fork (`./gradlew releaseTarGz`).
2. Bakes a Kafka container image with the REST-proxy-enabled libs.
3. Builds the Strimzi operator image from this branch (patching
   `kafka-versions.yaml` with the real SHA-512 of the tarball).
4. Loads both images into the local cluster (Docker Desktop k8s / kind / minikube).
5. Installs the Strimzi operator into the `kafka` namespace.
6. Applies the Kafka CR from `../examples/kafka/kafka-rest-proxy.yaml`.
7. Waits for the LoadBalancer Service to receive an external address.
8. Runs a `curl` with HTTP Basic Auth and asserts 200 + `partition`/`offset`.

First run is ~30–40 minutes (two Docker image builds plus an `mvn package`
of five Strimzi modules); re-runs with `--skip-kafka-build --skip-image-build`
finish in under a minute.
