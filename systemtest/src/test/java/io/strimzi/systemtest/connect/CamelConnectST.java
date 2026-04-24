/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.connect.build.MavenArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

/**
 * System tests for the Camel Kafka Connect pipeline.
 *
 * <p>These tests verify that Strimzi can build a KafkaConnect image containing
 * camel-kafka-connector plugins (downloaded from Maven at build time) and that
 * the resulting connectors produce and consume records correctly on a live
 * Kubernetes cluster.</p>
 *
 * <p>Three scenarios are covered, each building on a different connector type:</p>
 * <ol>
 *   <li><b>camel-timer-source</b> — self-generating source, no external system</li>
 *   <li><b>camel-counter-source</b> — self-generating source with sequential data</li>
 *   <li><b>camel-http-secured-sink</b> — sink that forwards records via HTTP POST
 *       to our custom REST proxy (port 9095) using Basic Auth</li>
 * </ol>
 */
@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@SuiteDoc(
    description = @Desc("Verifies Kafka Connect pipeline using camel-kafka-connector plugins "
            + "(timer source, counter source, http-secured sink) integrated with Strimzi."),
    beforeTestSteps = {
        @Step(value = "Deploy Kafka cluster and Cluster Operator.", expected = "Kafka cluster is ready.")
    },
    labels = {
        @Label(value = TestDocsLabels.CONNECT)
    }
)
class CamelConnectST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CamelConnectST.class);

    // All three connectors come from the same Maven group at version 4.8.0.
    // Strimzi's build system downloads these JARs and bundles them into the
    // KafkaConnect Docker image at deploy time — no pre-built image needed.
    private static final String CAMEL_VERSION = "4.8.0";
    private static final String CAMEL_GROUP = "org.apache.camel.kafkaconnector";

    // ttl.sh is a free ephemeral Docker registry (images expire after 24 h).
    // Strimzi pushes the built KafkaConnect image here so the cluster can pull it.
    private static final String CONNECT_IMAGE = "ttl.sh/strimzi-camel-connect:24h";

    /**
     * Test path:
     *   camel-timer-source Kamelet
     *       → fires every 2 s with a fixed string
     *       → KafkaConnect writes records to {@code timer-topic}
     *
     * <p>What it tests: Strimzi can build a KafkaConnect image containing the
     * camel-timer-source plugin and the connector starts without errors.
     * The timer fires autonomously — no external producer needed.</p>
     *
     * <p>Expected result: at least 3 records appear in {@code timer-topic}
     * within 60 s, each with value {@code "hello from camel connect"}.</p>
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that the camel-timer-source connector "
                + "produces messages to a Kafka topic automatically."),
        steps = {
            @Step(value = "Deploy KafkaConnect with camel-timer-source plugin built from Maven.",
                    expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy timer-connector KafkaConnector CR.",
                    expected = "Connector status is RUNNING."),
            @Step(value = "Consume messages from timer-topic.",
                    expected = "At least 3 messages received within 60 s.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelTimerSourceProducesMessages() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String timerTopic = "timer-topic-" + testStorage.getClusterName();

        // camel-timer-source Kamelet config:
        //   message — the string value written to every Kafka record
        //   period  — how often the timer fires (milliseconds)
        Plugin timerPlugin = new PluginBuilder()
                .withName("camel-timer-source")
                .withArtifacts(new MavenArtifactBuilder()
                        .withGroup(CAMEL_GROUP)
                        .withArtifact("camel-timer-source-kafka-connector")
                        .withVersion(CAMEL_VERSION)
                        .build())
                .build();

        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(),
                        testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(),
                        testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), timerTopic,
                        testStorage.getClusterName()).build()
        );
        // Strimzi builds the KafkaConnect Docker image here by downloading the
        // Maven artifact, overlaying it on the base image, and pushing to ttl.sh.
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectTemplates.kafkaConnectWithBuildAndWait(
                        testStorage.getNamespaceName(),
                        testStorage.getClusterName(),
                        1,
                        CONNECT_IMAGE,
                        List.of(timerPlugin)
                ).build()
        );

        // StringConverter at the worker level avoids a classpath conflict:
        // camel-kafka-connector 4.8.0 bundles Kafka 3.9.1 which has an
        // incompatible JsonConverter with the worker's Kafka 4.2.0.
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(),
                        "timer-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.timersource.CamelTimersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", timerTopic,
                                    "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.timer-source.message", "hello from camel connect",
                                    "camel.kamelet.timer-source.period", "2000"
                            ))
                        .endSpec()
                        .build()
        );

        // Wait until the KafkaConnector CR reports state=RUNNING before consuming,
        // otherwise the consumer may time out before any records are produced.
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "timer-connector");

        // Consume 3 records from timer-topic.
        // Expected: 3 records each containing "hello from camel connect" arrive
        // within 60 s (timer fires every 2 s, so 3 records = ~6 s in practice).
        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withBootstrapAddress(testStorage.getClusterName() + "-kafka-bootstrap:9092")
                .withTopicName(timerTopic)
                .withMessageCount(3)
                .withConsumerGroup("timer-test-consumer-group")
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(),
                kafkaClients.getConsumerName(), 3, 60_000);

        LOGGER.info("Timer source connector successfully produced messages to topic: {}", timerTopic);
    }

    /**
     * Test path:
     *   camel-counter-source Kamelet
     *       → emits integers 1, 2, 3, 4, 5 (one per second)
     *       → KafkaConnect writes records to {@code counter-topic}
     *
     * <p>What it tests: the counter connector produces structured, varying data
     * (not just a fixed string) and respects the {@code start} and {@code numbers}
     * Kamelet parameters that control the sequence range.</p>
     *
     * <p>Expected result: exactly 5 records appear in {@code counter-topic},
     * with values {@code "1"}, {@code "2"}, {@code "3"}, {@code "4"}, {@code "5"}
     * in order.</p>
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that the camel-counter-source connector "
                + "produces sequential integer messages to a Kafka topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with camel-counter-source plugin built from Maven.",
                    expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy counter-connector KafkaConnector CR.",
                    expected = "Connector status is RUNNING."),
            @Step(value = "Consume 5 messages from counter-topic.",
                    expected = "5 messages received with sequential values starting from 1.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelCounterSourceProducesSequentialNumbers() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String counterTopic = "counter-topic-" + testStorage.getClusterName();
        final int messageCount = 5;

        // camel-counter-source Kamelet config:
        //   start   — first integer in the sequence (inclusive)
        //   period  — delay between each integer (milliseconds)
        //   numbers — total number of integers to emit before stopping
        Plugin counterPlugin = new PluginBuilder()
                .withName("camel-counter-source")
                .withArtifacts(new MavenArtifactBuilder()
                        .withGroup(CAMEL_GROUP)
                        .withArtifact("camel-counter-source-kafka-connector")
                        .withVersion(CAMEL_VERSION)
                        .build())
                .build();

        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(),
                        testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(),
                        testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), counterTopic,
                        testStorage.getClusterName()).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectTemplates.kafkaConnectWithBuildAndWait(
                        testStorage.getNamespaceName(),
                        testStorage.getClusterName(),
                        1,
                        CONNECT_IMAGE,
                        List.of(counterPlugin)
                ).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(),
                        "counter-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.countersource.CamelCountersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", counterTopic,
                                    "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.counter-source.start", "1",
                                    "camel.kamelet.counter-source.period", "1000",
                                    // Stop after exactly messageCount integers so the consumer
                                    // does not wait indefinitely for records that never arrive.
                                    "camel.kamelet.counter-source.numbers", String.valueOf(messageCount)
                            ))
                        .endSpec()
                        .build()
        );

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "counter-connector");

        // Consume exactly messageCount records from counter-topic.
        // Expected: records arrive with values "1" through "5" in order
        // within 60 s (1 record/s, so ~5 s in practice).
        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withBootstrapAddress(testStorage.getClusterName() + "-kafka-bootstrap:9092")
                .withTopicName(counterTopic)
                .withMessageCount(messageCount)
                .withConsumerGroup("counter-test-consumer-group")
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(),
                kafkaClients.getConsumerName(), messageCount, 60_000);

        LOGGER.info("Counter source connector successfully produced {} sequential messages to topic: {}",
                messageCount, counterTopic);
    }

    /**
     * Test path (full end-to-end pipeline):
     * <pre>
     *   camel-timer-source Kamelet
     *       → fires every 3 s
     *       → KafkaConnect writes records to {@code timer-topic}
     *       ↓
     *   camel-http-secured-sink Kamelet
     *       → reads records from {@code timer-topic}
     *       → HTTP POST to REST proxy at port 9095 with Basic Auth (alice:s3cret)
     *       ↓
     *   Custom Kafka REST proxy (embedded in broker)
     *       → authenticates the request
     *       → produces the record value to {@code demo-topic}
     * </pre>
     *
     * <p>What it tests: the full integration between Kafka Connect and our
     * custom REST proxy. The sink connector must:</p>
     * <ul>
     *   <li>Send HTTP POST with no explicit {@code Content-Type} header
     *       (the Kamelet does not set one) — the REST proxy must accept
     *       it and treat the raw body as the record value.</li>
     *   <li>Include {@code Authorization: Basic YWxpY2U6czNjcmV0} on
     *       the first request ({@code authenticationPreemptive: true})
     *       without waiting for a 401 challenge.</li>
     * </ul>
     *
     * <p>Expected result: at least 3 records appear in {@code demo-topic}
     * within 120 s, confirming the complete chain works end-to-end.</p>
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies full end-to-end pipeline: timer-source produces to "
                + "timer-topic, http-secured-sink forwards to REST proxy, "
                + "messages land in demo-topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with timer-source and http-secured-sink plugins.",
                    expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy timer-connector producing to timer-topic.",
                    expected = "Connector status is RUNNING."),
            @Step(value = "Deploy http-secured-sink-connector reading from timer-topic, "
                    + "POSTing to REST proxy.",
                    expected = "Connector status is RUNNING."),
            @Step(value = "Consume from demo-topic.",
                    expected = "Messages arrive in demo-topic via REST proxy — full pipeline verified.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelHttpSecuredSinkForwardsToRestProxy() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String timerTopic = "timer-topic-" + testStorage.getClusterName();
        final String demoTopic = "demo-topic";

        // The REST proxy is embedded in the Kafka broker and listens on port 9095.
        // my-cluster-kafka-rest-bootstrap is the LoadBalancer Service created by
        // Strimzi for the HTTP listener defined in the Kafka CR.
        final String restProxyUrl =
                "http://my-cluster-kafka-rest-bootstrap.kafka.svc:9095/v1/topics/" + demoTopic;

        // Both plugins are bundled into the same KafkaConnect image.
        // timer-source produces records; http-secured-sink consumes and forwards them.
        Plugin timerPlugin = new PluginBuilder()
                .withName("camel-timer-source")
                .withArtifacts(new MavenArtifactBuilder()
                        .withGroup(CAMEL_GROUP)
                        .withArtifact("camel-timer-source-kafka-connector")
                        .withVersion(CAMEL_VERSION)
                        .build())
                .build();

        Plugin httpSinkPlugin = new PluginBuilder()
                .withName("camel-http-secured-sink")
                .withArtifacts(new MavenArtifactBuilder()
                        .withGroup(CAMEL_GROUP)
                        .withArtifact("camel-http-secured-sink-kafka-connector")
                        .withVersion(CAMEL_VERSION)
                        .build())
                .build();

        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(),
                        testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(),
                        testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), timerTopic,
                        testStorage.getClusterName()).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectTemplates.kafkaConnectWithBuildAndWait(
                        testStorage.getNamespaceName(),
                        testStorage.getClusterName(),
                        1,
                        CONNECT_IMAGE,
                        List.of(timerPlugin, httpSinkPlugin)
                ).build()
        );

        // Connector 1 — source side.
        // Produces JSON-shaped records so the REST proxy can parse key + value.
        // Period is 3000 ms; 3 records arrive in ~9 s.
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(),
                        "timer-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.timersource.CamelTimersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", timerTopic,
                                    "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.timer-source.message",
                                            "{\"key\":\"camel\",\"value\":\"hello from timer\"}",
                                    "camel.kamelet.timer-source.period", "3000"
                            ))
                        .endSpec()
                        .build()
        );

        // Connector 2 — sink side.
        // Reads each record from timer-topic and POSTs its value to the REST proxy.
        // authenticationPreemptive=true means the Authorization header is sent on
        // the very first request without waiting for a 401 challenge — required
        // because our REST proxy does not issue a WWW-Authenticate challenge.
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(),
                        "http-secured-sink-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.httpsecuredsink.CamelHttpsecuredsinkSinkConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", timerTopic,
                                    "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.http-secured-sink.url", restProxyUrl,
                                    "camel.kamelet.http-secured-sink.method", "POST",
                                    "camel.kamelet.http-secured-sink.authMethod", "Basic",
                                    "camel.kamelet.http-secured-sink.authUsername", "alice",
                                    "camel.kamelet.http-secured-sink.authPassword", "s3cret",
                                    "camel.kamelet.http-secured-sink.authenticationPreemptive", "true"
                            ))
                        .endSpec()
                        .build()
        );

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "timer-connector");
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "http-secured-sink-connector");

        // Consume from demo-topic — this is the final destination after the REST proxy.
        // If 3 records arrive here, the full pipeline is verified:
        //   timer-source produced → timer-topic held → http-secured-sink forwarded
        //   → REST proxy authenticated and wrote → demo-topic received.
        // Timeout is 120 s (longer than the source-only tests) because the pipeline
        // has two connector hops plus an HTTP round-trip before records land here.
        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withBootstrapAddress("my-cluster-kafka-bootstrap.kafka.svc:9092")
                .withTopicName(demoTopic)
                .withMessageCount(3)
                .withConsumerGroup("e2e-test-consumer-group")
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(),
                kafkaClients.getConsumerName(), 3, 120_000);

        LOGGER.info("Full e2e pipeline verified: timer-topic -> http-secured-sink -> REST proxy -> demo-topic");
    }

    @BeforeAll
    void setup() {
        // Cluster Operator is set up per test via @ParallelNamespaceTest — no global setup needed.
    }
}
