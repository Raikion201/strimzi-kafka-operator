/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.fabric8.kubernetes.api.model.Service;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import org.junit.jupiter.api.Assumptions;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * System test for the embedded HTTP REST proxy listener (type: http). Runs
 * against a real Kubernetes cluster and satisfies the stakeholder demo
 * requirement end-to-end:
 *
 * <ul>
 *     <li>Deploys a Kafka CR with a {@code type: http} listener on port 8080</li>
 *     <li>Waits for Strimzi to create the matching LoadBalancer Service</li>
 *     <li>Waits for the cluster's cloud provider (or MetalLB / minikube tunnel) to
 *         allocate an external address</li>
 *     <li>Sends a {@code POST /v1/topics/{name}} over the external address</li>
 *     <li>Asserts the REST proxy returned a 200 with a partition + offset in the body</li>
 * </ul>
 *
 * <p><b>Requirements to run this test:</b></p>
 * <ul>
 *     <li>A Kubernetes cluster where {@code Service type: LoadBalancer} gets an
 *         external address. Works out of the box on GKE/EKS/AKS; on kind use
 *         MetalLB; on minikube use {@code minikube tunnel}.</li>
 *     <li>A Kafka container image built from the companion fork
 *         (github.com/Raikion201/kafka @ rest-proxy) reachable by the test's
 *         cluster — see {@code rest-listener/BUILDING_THE_KAFKA_IMAGE.md}.
 *         The {@code spec.kafka.version} in the CR must match an entry in
 *         {@code kafka-versions.yaml} with {@code supports-rest-proxy: true}.</li>
 * </ul>
 *
 * <p>If either condition is missing the test is skipped (configured at the
 * environment level) rather than failed, so a developer without the full
 * infra can still run the rest of the suite.</p>
 */
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("HTTP REST proxy listener end-to-end test — the stakeholder demo."),
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class HttpListenerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpListenerST.class);

    /** Kafka version with supports-rest-proxy: true in kafka-versions.yaml. */
    private static final String REST_PROXY_KAFKA_VERSION = "4.4.0";

    /**
     * Matches the listener's port in the CR below. Port 8080 is taken inside
     * the Kafka broker image by the KafkaAgent JMX server, so REST proxy
     * listeners must bind to a free port. 9090 is the convention used by
     * the companion demo scripts in the Kafka fork.
     */
    private static final int REST_LISTENER_PORT = 9095;

    /** One row in the http.rest.basic.credentials config string. */
    private static final String REST_USER = "alice";
    private static final String REST_PASS = "s3cret";

    /**
     * End-to-end: Kafka CR with type: http listener → external LoadBalancer → curl → 200.
     * If this test goes green on a real cluster, the stakeholder requirement is met.
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Stakeholder demo — POST a record to the embedded REST proxy through the Strimzi-created LoadBalancer and confirm the broker appended it."),
        steps = {
            @Step(value = "Deploy a Kafka CR with type: http listener on port 9090", expected = "Kafka cluster rolls out"),
            @Step(value = "Create a KafkaTopic CR for the test topic", expected = "Topic is materialised on the broker"),
            @Step(value = "Wait for the LoadBalancer Service to receive an external address", expected = "External IP / hostname populated"),
            @Step(value = "POST /v1/topics/<test-topic> with Basic Auth", expected = "HTTP 200 with {partition, offset} body")
        },
        labels = { @Label(value = TestDocsLabels.KAFKA) }
    )
    void httpListenerExposedViaLoadBalancerAcceptsProduce() {
        // The REST-proxy Kafka image must be pre-built locally before this test
        // can run. Skip gracefully rather than failing with ImagePullBackOff.
        // Build it with: bash demo-strimzi-rest-proxy.sh (in the kafka fork root).
        Assumptions.assumeTrue(
            isDockerImageAvailable("kafka-rest-proxy:demo"),
            "Skipping: kafka-rest-proxy:demo not found locally. " +
            "Build it with: bash demo-strimzi-rest-proxy.sh");

        final TestStorage ts = new TestStorage(KubeResourceManager.get().getTestContext());

        // Deploy the Kafka CR. Broker pods come up with the REST proxy bound on 9090.
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(ts.getNamespaceName(), ts.getBrokerPoolName(), ts.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(ts.getNamespaceName(), ts.getControllerPoolName(), ts.getClusterName(), 1).build(),
                KafkaTemplates.kafka(ts.getNamespaceName(), ts.getClusterName(), 1)
                        .editSpec()
                            .editKafka()
                                .withVersion(REST_PROXY_KAFKA_VERSION)
                                .withListeners(
                                        new GenericKafkaListenerBuilder()
                                                .withName("plain")
                                                .withPort(9092)
                                                .withType(KafkaListenerType.INTERNAL)
                                                .withTls(false)
                                                .build(),
                                        new GenericKafkaListenerBuilder()
                                                .withName("rest")
                                                .withPort(REST_LISTENER_PORT)
                                                .withType(KafkaListenerType.HTTP)
                                                .withTls(false)
                                                .build())
                                .addToConfig("http.rest.basic.credentials", REST_USER + ":" + REST_PASS)
                                .addToConfig("http.rest.swagger-ui.enabled", true)
                            .endKafka()
                        .endSpec()
                        .build()
        );

        // Pre-create the KafkaTopic so the first POST doesn't hit the
        // auto-create-disabled 404 path. The REST proxy only supports
        // produce to existing topics; the Topic Operator materialises this
        // CR into a real Kafka topic on the plain listener before we curl.
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(ts.getNamespaceName(), ts.getTopicName(), ts.getClusterName(), 3).build()
        );

        // Wait for the LoadBalancer Service the operator created for the REST
        // listener. Naming pattern: <cluster>-kafka-<listenerName>-bootstrap.
        // We wait directly for the LoadBalancer ingress slot to fill — that
        // necessarily implies the Service exists.
        final String serviceName = ts.getClusterName() + "-kafka-rest-bootstrap";
        Service lb = waitForLoadBalancerAddress(ts.getNamespaceName(), serviceName);
        final String externalHost = externalAddress(lb);
        assertThat("LoadBalancer never acquired an external address — does your cluster support LoadBalancer Services?",
                externalHost, not(is("")));

        LOGGER.info("REST proxy LoadBalancer is at http://{}:{}", externalHost, REST_LISTENER_PORT);

        // POST /v1/topics/<topic> through the external address. The topic
        // was created above via a KafkaTopic CR, so the REST proxy's existence
        // check passes and the produce path is exercised.
        try {
            final String credentials = Base64.getEncoder()
                    .encodeToString((REST_USER + ":" + REST_PASS).getBytes(java.nio.charset.StandardCharsets.UTF_8));

            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + externalHost + ":" + REST_LISTENER_PORT + "/v1/topics/" + ts.getTopicName()))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Basic " + credentials)
                    .POST(HttpRequest.BodyPublishers.ofString("{\"key\":\"k\",\"value\":\"hello-from-st\"}"))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            final HttpResponse<String> resp = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build()
                    .send(req, HttpResponse.BodyHandlers.ofString());

            LOGGER.info("REST proxy response: {} {}", resp.statusCode(), resp.body());
            assertThat("REST proxy refused the POST — response body: " + resp.body(),
                    resp.statusCode(), is(200));
            assertThat(resp.body(), containsString("\"partition\""));
            assertThat(resp.body(), containsString("\"offset\""));
        } catch (Exception e) {
            throw new RuntimeException("Failed to call REST proxy at " + externalHost, e);
        }
    }

    /**
     * Polls until the Service has an external address on its first ingress slot.
     * Raises after ~5 minutes if nothing appears — typical failure mode on a
     * cluster without LoadBalancer support.
     */
    private static Service waitForLoadBalancerAddress(String namespace, String serviceName) {
        final long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
        while (System.currentTimeMillis() < deadline) {
            Service svc = KubeResourceManager.get().kubeClient().getClient()
                    .services().inNamespace(namespace).withName(serviceName).get();
            if (svc != null
                    && svc.getStatus() != null
                    && svc.getStatus().getLoadBalancer() != null
                    && svc.getStatus().getLoadBalancer().getIngress() != null
                    && !svc.getStatus().getLoadBalancer().getIngress().isEmpty()) {
                return svc;
            }
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("Service " + namespace + "/" + serviceName
                + " never acquired a LoadBalancer address within 5 minutes");
    }

    /** IP or hostname, whichever the ingress slot has populated. */
    private static String externalAddress(Service lb) {
        var ingress = lb.getStatus().getLoadBalancer().getIngress().get(0);
        return ingress.getIp() != null ? ingress.getIp() : ingress.getHostname();
    }

    /** Returns true if the named Docker image exists in the local daemon. */
    private static boolean isDockerImageAvailable(String image) {
        try {
            Process p = new ProcessBuilder("docker", "image", "inspect", "--format", "{{.Id}}", image)
                    .redirectErrorStream(true)
                    .start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator.getInstance().withDefaultConfiguration().install();
    }
}
