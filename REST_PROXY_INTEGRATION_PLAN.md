# Strimzi × Kafka REST Proxy — Integration Plan

Branch: `rest-api` (this repo)
Companion: https://github.com/Raikion201/kafka @ `rest-proxy`

---

## Goal (one sentence)

`kubectl apply -f kafka.yaml` brings up a Strimzi-managed Kafka cluster
where every broker runs the embedded REST proxy, and Strimzi itself
creates a LoadBalancer (or Ingress/Route) exposing the HTTP port so that
`curl http://<strimzi-lb>/v1/topics/foo` from outside the cluster
produces `{"partition":N,"offset":N}`.

## Fixed inputs (do not redesign)

| Thing | Value |
|---|---|
| Broker activation | `listeners=...,HTTP://0.0.0.0:8080,HTTPS://0.0.0.0:8443` in `server.properties` |
| Listener names | literal `HTTP` / `HTTPS` (matched by `KafkaConfig.HttpListenerRegex`) |
| Not in `listener.security.protocol.map` | The broker side strips HTTP/HTTPS before that config is built |
| Config keys | `http.rest.executor.threads` (default 8), `http.rest.basic.credentials`, `http.rest.swagger-ui.enabled` |
| One route | `POST /v1/topics/{name}` |

Anything in this table is a given — the plan makes Strimzi emit the above
correctly; it does not re-litigate the broker design.

## Project discipline

1. **No new files inside existing Strimzi modules.** All new Java goes
   into a new top-level module, `rest-listener`.
2. **Existing files may be modified** to dispatch into the new module,
   but the existing listener types (`internal`, `route`, `loadbalancer`,
   `nodeport`, `ingress`, `cluster-ip`) keep behaving byte-for-byte the
   same.
3. **Match the existing Strimzi patterns** — Maven module layout mirrors
   `certificate-manager`; package under `io.strimzi.operator.*`;
   checkstyle, spotbugs, `pmd` all on.

This is the same discipline the companion Kafka fork followed: `:core`
got no new files, all new code lives in `:http` + `:http-api`.

---

## New module: `rest-listener/`

```
rest-listener/
├── pom.xml                          ← mirrors certificate-manager/pom.xml
└── src/
    ├── main/java/io/strimzi/operator/cluster/rest/
    │   ├── HttpListenerTypeSupport.java   ← predicates "is this HTTP/HTTPS?"
    │   ├── HttpListenerConfigurer.java    ← renders server.properties snippet
    │   ├── HttpListenerServices.java      ← builds Service / Ingress / Route
    │   └── HttpListenerValidator.java     ← CR-level validation rules
    └── test/java/io/strimzi/operator/cluster/rest/
        ├── HttpListenerConfigurerTest.java
        ├── HttpListenerServicesTest.java
        └── HttpListenerValidatorTest.java
```

**Compile deps:** `api`, `operator-common` (for `GenericKafkaListener`,
`ServiceUtils`), `fabric8-kubernetes-api`. No reconcile-loop dependency.
The module is pure logic; the operator calls into it.

## Modifications to existing files

Every change below is a dispatch into the new module — never new logic
in-place.

| File | Edit | Approx lines |
|---|---|---|
| `pom.xml` | add `<module>rest-listener</module>` | 1 |
| `cluster-operator/pom.xml` | add `<dependency>rest-listener</dependency>` | 5 |
| `api/.../listener/KafkaListenerType.java` | add `HTTP`, `HTTPS` to enum + `forValue`/`toValue` | 10 |
| `cluster-operator/.../KafkaBrokerConfigurationBuilder.java` | one `if (HttpListenerTypeSupport.isHttpOrHttps(listener)) { HttpListenerConfigurer.configure(writer, listener); continue; }` at the start of the listener loop (line 279) | 4 |
| `cluster-operator/.../KafkaCluster.java` | dispatch inside `generateExternalBootstrapServices` for HTTP/HTTPS listeners, calling `HttpListenerServices.bootstrapService(...)` | 6 |
| `cluster-operator/.../ListenersValidator.java` | one `errors.addAll(HttpListenerValidator.validate(listener))` in the per-listener validation loop | 2 |
| `kafka-versions.yaml` | add `4.4.0-rest-proxy` entry pointing at the forked tarball's GitHub Release URL | 8 |
| `packaging/install/cluster-operator/*.yaml` | **regenerated** from the annotation change; not hand-edited | (auto) |

Nothing else in the tree is edited. No new file appears under
`api/src/main/`, `cluster-operator/src/main/`, `operator-common/src/main/`,
or anywhere else that already exists.

---

## Phase-by-phase milestones (what you can demo at each)

### Phase 1 — Module skeleton + enum (this commit)

- `rest-listener/` module created, `pom.xml`, empty stubs.
- `KafkaListenerType` enum gains `HTTP` and `HTTPS`.
- CRD regenerated so `type: http` / `type: https` pass schema validation.

**Demo:** `kubectl apply` of a `Kafka` CR with `type: http` is accepted
(no more "unknown type" validation error). The HTTP listener is ignored
by the operator for now — nothing binds, nothing curls.

### Phase 2 — Config rendering

- `HttpListenerConfigurer.configure(writer, listener)` writes the
  `HTTP://0.0.0.0:8080` snippet correctly: appended to `listeners=`,
  NOT appended to `listener.security.protocol.map`.
- `KafkaBrokerConfigurationBuilder` dispatches to it.
- Unit test: render config with a `type: http` listener, assert
  `listeners=...HTTP://0.0.0.0:8080...` and absence of `HTTP:` in the
  protocol map.

**Demo:** broker Pod starts and binds 8080 internally. From inside the
cluster (`kubectl exec` into another pod): `curl ...kafka-rest-bootstrap:8080/v1/topics/foo`
returns a 401 (auth required) — proof of life. External access not yet.

### Phase 3 — Services / LoadBalancer / Ingress

- `HttpListenerServices.bootstrapService(listener)` and
  `.perBrokerService(listener, brokerIndex)` implement Service generation
  for HTTP/HTTPS types. `configuration.bootstrap.loadBalancerIP` →
  `type: LoadBalancer`; `configuration.ingressClassName` → Ingress;
  default → `ClusterIP`.
- `KafkaCluster.generateExternalBootstrapServices` delegates.

**Demo:** **the boss's requirement is met.**
`kubectl get svc <cluster>-kafka-rest-bootstrap` → External IP.
`curl http://<external-ip>:8080/v1/topics/foo` → 200 from outside.

### Phase 4 — Validation

- `HttpListenerValidator` enforces HTTPS requires `tls: true`, HTTP
  forbids `tls: true`, auth types other than HTTP Basic are rejected,
  ports unique.
- `ListenersValidator` delegates.

### Phase 5 — Custom Kafka version wired in

- `kafka-versions.yaml` gains `4.4.0-rest-proxy` pointing at a GitHub
  Release of the companion-fork tarball.
- `KafkaVersion.supportsRestProxy()` gate: HTTP/HTTPS listener types
  against an unsupporting version produce a clear validation error.

### Phase 6 — Systemtest

- New `HttpListenerST` under `systemtest/`. Deploys a `Kafka` CR with an
  HTTP listener, waits for the LoadBalancer Service, does the produce →
  consume round-trip over the external endpoint. This is the automated
  version of the Phase 3 demo.

### Phase 7 — Docs & example

- `examples/kafka/kafka-rest-proxy.yaml` — minimal CR.
- `documentation/.../assembly-kafka-listeners.adoc` — new subsection.
- `CHANGELOG.md` entry.

---

## Test plan

| Tier | Where | What | When |
|---|---|---|---|
| **Unit** | `rest-listener/src/test/` | Per-class, config render correctness, Service shape, validation rules | Ship with each phase |
| **Operator reconcile (mocked k8s)** | `cluster-operator/src/test/.../KafkaAssemblyOperatorMockTest.java` | Given a CR with HTTP listener, reconcile and assert the mocked API saw the right `create Service` call | Phase 3 |
| **Systemtest** | `systemtest/src/test/.../HttpListenerST.java` | Real k8s (kind / minikube), real `curl` through the LoadBalancer | Phase 6 |

Roughly: 20 unit tests in `rest-listener`, 2 reconcile tests in
`cluster-operator`, 2 systemtests. The systemtest at Phase 6 is the one
the stakeholder will ask to see running.

## Out of scope

- REST consumer / offset-commit endpoints (the proxy is single-produce by
  design).
- OAuth / mTLS on the REST listener (Basic Auth only for v1).
- Kafka upstream KIP process (long-term path to eliminate the fork).

## Risks

| Risk | Mitigation |
|---|---|
| Fork drift from Apache Kafka `trunk` | Rebase schedule; long-term KIP |
| Strimzi `main` moves while branch is in progress | Rebase per phase commit |
| Unknown Kubernetes provider differences (Ingress controllers etc.) | Default to LoadBalancer for the first demo; Ingress is optional config |

## First commit

This commit lands Phase 1 only: the new module with stubs, enum change,
parent-POM registration. Subsequent phases are separate commits so
reviewers can read them linearly.
