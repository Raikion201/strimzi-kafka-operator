# Building a Strimzi Kafka image that carries the embedded REST proxy

The operator code in this branch drives the Kubernetes side — it tells
Strimzi how to wire an `HTTP://` / `HTTPS://` listener, create the
`Service` / `LoadBalancer`, and validate the CR. **None of that helps
unless the broker container actually contains the REST proxy classes.**
Stock upstream Kafka doesn't have them; you have to build a Kafka
tarball from the companion fork and point Strimzi at it.

This document spells out that build-and-publish recipe end to end.

## The companion Kafka fork

Source: https://github.com/Raikion201/kafka branch `rest-proxy`.

Lives as two Gradle modules — `:http-api` (interfaces / SPI seam) and
`:http` (Jetty/Jersey implementation, discovered at runtime via
`ServiceLoader`). When the broker sees an `HTTP://` or `HTTPS://` entry
in `listeners=`, the embedded server starts on that port and exposes
`POST /v1/topics/{name}`.

## Step 1 — cut a tarball from the fork

```bash
git clone https://github.com/Raikion201/kafka.git
cd kafka
git checkout rest-proxy

# Same command Apache uses to cut release tarballs. Produces:
#   core/build/distributions/kafka_2.13-4.4.0-SNAPSHOT.tgz
./gradlew releaseTarGz -PscalaVersion=2.13
```

If the build succeeds you'll have a tarball under
`core/build/distributions/`. Inspect it to confirm the REST proxy jars
are present:

```bash
tar tzf core/build/distributions/kafka_2.13-*.tgz | grep kafka-http
# Expected:
#   kafka_2.13-4.4.0-SNAPSHOT/libs/kafka-http-api-4.4.0-SNAPSHOT.jar
#   kafka_2.13-4.4.0-SNAPSHOT/libs/kafka-http-4.4.0-SNAPSHOT.jar
```

If those two jars are missing, the fork is out of date or the build
skipped the `:http` module — investigate before publishing.

## Step 2 — rename + checksum + publish

Strimzi's `kafka-versions.yaml` entry expects a specific URL and the
SHA-512 of the file it downloads. Rename the tarball to match the
version string you'll put in `kafka-versions.yaml`:

```bash
cd core/build/distributions
mv kafka_2.13-4.4.0-SNAPSHOT.tgz kafka_2.13-4.4.0-rest-proxy.tgz
sha512sum kafka_2.13-4.4.0-rest-proxy.tgz
```

Publish the tarball somewhere addressable over HTTPS. Options:

- **GitHub Release on the fork** — simplest. Tag the fork (`rest-proxy-0.1`),
  attach the tarball as a release asset. URL pattern:
  `https://github.com/Raikion201/kafka/releases/download/rest-proxy-0.1/kafka_2.13-4.4.0-rest-proxy.tgz`
- **Internal artifact store (Nexus, S3, Artifactory)** — equivalent. Any
  HTTPS URL works; Strimzi just `curl`s it during image build.

## Step 3 — update `kafka-versions.yaml`

The entry is already there (added in Phase 5a); patch in the real
checksum from Step 2:

```yaml
- version: 4.4.0-rest-proxy
  metadata: 4.2
  url: https://github.com/Raikion201/kafka/releases/download/rest-proxy-0.1/kafka_2.13-4.4.0-rest-proxy.tgz
  checksum: <SHA-512 from step 2>
  third-party-libs: 4.2.x
  supported: true
  default: false
  supports-rest-proxy: true
```

If you change the URL or tarball contents, update the checksum in the
same commit — Strimzi's image build refuses the download if the SHA
doesn't match.

## Step 4 — build the Strimzi Kafka image

From the Strimzi repo root:

```bash
make -C docker-images/kafka-based build
```

That invokes `docker-images/kafka-based/build.sh`, which reads
`kafka-versions.yaml`, `curl`s the URL, verifies the checksum, unpacks
the tarball, and builds the container image. On success you get local
images like:

```
quay.io/strimzi/kafka:latest-kafka-4.4.0-rest-proxy
```

## Step 5 — push to a registry your cluster can pull from

```bash
docker tag quay.io/strimzi/kafka:latest-kafka-4.4.0-rest-proxy \
           <your-registry>/kafka:rest-proxy-demo
docker push <your-registry>/kafka:rest-proxy-demo
```

For a quick local demo (kind / minikube), `docker save` + `kind load` /
`minikube image load` skips the registry entirely.

## Step 6 — point the Kafka CR at it

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: demo
spec:
  kafka:
    version: 4.4.0-rest-proxy                  # must match kafka-versions.yaml
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: rest
        port: 8080
        type: http                             # the new type this branch adds
        tls: false
    config:
      http.rest.basic.credentials: alice:s3cret
      http.rest.executor.threads: 8
```

If the operator has been deployed with this branch's code, `kubectl apply`
produces a `LoadBalancer` Service named `demo-kafka-rest-bootstrap`, the
broker pod binds port 8080 internally, and `curl` against the
LoadBalancer's external IP gets through to the embedded proxy.

## How the upstream KIP would change this

Long-term the REST proxy should land in Apache Kafka itself. When that
happens, delete the fork, drop the forked entry from
`kafka-versions.yaml` (replaced by the stock upstream version's entry
with `supports-rest-proxy: true`), and this document becomes obsolete.
Until then, the fork + tarball path above is the deal.
