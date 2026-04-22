# `rest-listener` — Strimzi support for the embedded Kafka REST proxy

This module adds two new listener types to Strimzi's `Kafka` CRD:

| `type` | Purpose |
|---|---|
| `http`  | Plain HTTP REST proxy listener — serves `POST /v1/topics/{name}` |
| `https` | Same surface, wrapped in TLS via Kafka's `SslFactory` |

They are only usable with a Kafka build that has the embedded REST proxy
baked in (see `BUILDING_THE_KAFKA_IMAGE.md` for the forked distribution).

## Using it in a Kafka CR

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

A full copy-paste example is at [`examples/kafka/kafka-rest-proxy.yaml`](../examples/kafka/kafka-rest-proxy.yaml).

## What the operator does with a `type: http` listener

Given a listener entry like the above, the operator:

1. Writes `listeners=...,HTTP://0.0.0.0:8080` into the broker's
   `server.properties`. **It does NOT** add an entry to
   `listener.security.protocol.map` — the embedded proxy sits outside
   Kafka's security-protocol machinery.
2. Creates a Kubernetes `Service` named `<cluster>-kafka-<listenerName>-bootstrap`
   with `spec.type: LoadBalancer` (by default) targeting the broker pods
   on the listener's port.
3. Does **not** generate per-broker Services for REST listeners — REST
   requests work against any broker, so the bootstrap Service alone is
   enough.
4. Rejects the CR at admission time if `spec.kafka.version` does not
   support the REST proxy, with a clear error message.

### HTTPS listener

An HTTPS listener is the same plus:

- `tls: true` (enforced by the validator; plain HTTP listeners must have
  `tls: false`).
- Optional custom cert via `configuration.brokerCertChainAndKey` —
  reuses Strimzi's existing per-listener TLS plumbing; the generated
  `listener.name.https.ssl.keystore.*` keys match what the broker's
  `KafkaSslContextFactory` looks up.

## Validation rules

Enforced at CR admission time by `HttpListenerValidator`:

- HTTPS listener must have `tls: true`; HTTP must have `tls: false`.
- Listener-level authentication (`authentication: { type: tls | scramSha512 | oauth }`)
  is **not** applicable — REST auth is HTTP Basic via
  `http.rest.basic.credentials`. Set it in `spec.kafka.config` (as in
  the example above).
- Kafka version must have `supports-rest-proxy: true`. Otherwise the
  REST code is not on the broker's classpath, and the broker would
  never bind the port.

## Why a separate module?

`cluster-operator` receives only narrow dispatch calls:

- `KafkaBrokerConfigurationBuilder` — one `if` at the top of the
  listener loop that delegates to `HttpListenerConfigurer`.
- `ListenersUtils.serviceType` — one delegation to `HttpListenerServices`.
- `KafkaCluster.generatePerPodServices` — a stream filter using
  `HttpListenerServices.skipPerPodService`.
- `KafkaCluster.fromCrd` — one call to
  `HttpListenerValidator.checkKafkaVersionSupport`.
- `ListenersValidator` — one call to
  `HttpListenerValidator.validate`.

Everything else HTTP-specific — rendering, Service shape, rule sets —
lives in this module. Drop `rest-listener/` and the operator falls back
to its pre-REST behaviour with zero changes elsewhere.

## Files

| Path | Role |
|---|---|
| `src/main/java/.../HttpListenerTypeSupport.java` | Predicates + wire-name derivation |
| `src/main/java/.../HttpListenerConfigurer.java`  | `server.properties` snippet |
| `src/main/java/.../HttpListenerServices.java`    | Service type + per-pod-service policy |
| `src/main/java/.../HttpListenerValidator.java`   | CR validation + version-support gate |
| `BUILDING_THE_KAFKA_IMAGE.md`                    | How to produce a Kafka tarball with the REST proxy from the fork |
| `src/test/java/.../*Test.java`                   | 26 unit tests covering the above |
