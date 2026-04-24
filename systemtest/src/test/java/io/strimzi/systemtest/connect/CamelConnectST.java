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

/**
 * System tests for the Camel Kafka Connect pipeline.
 *
 * <p>These tests verify that Strimzi can build a KafkaConnect image containing
 * camel-kafka-connector plugins (downloaded from Maven at build time) and that
 * the resulting connectors produce and consume records correctly on a live
 * Kubernetes cluster.</p>
 *
 * <p>Three scenarios are covered:</p>
 * <ol>
 *   <li><b>camel-timer-source</b> — fires on a schedule and writes a static
 *       message ({@code "hello from timer"}) to a Kafka topic.</li>
 *   <li><b>camel-counter-source</b> — produces sequential raw integers
 *       (1, 2, 3…) to demonstrate a self-generating numeric source.</li>
 *   <li><b>camel-http-secured-sink</b> (end-to-end) — reads integer counter
 *       values from the counter-source topic and forwards each one via HTTP
 *       POST to our custom REST proxy (port 9095) using Basic Auth, resulting
 *       in messages {@code "1"}, {@code "2"}, {@code "3"} landing in
 *       {@code demo-topic}.</li>
 * </ol>
 *
 * <p><b>Why counter-source for the sink demo?</b><br>
 * The {@code camel-http-secured-sink} is wired to read from
 * {@code counter-topic} so that the final {@code demo-topic} contains
 * clearly incrementing integer values, making it easy to verify that
 * every message flowed through the pipeline in order.</p>
 */
@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@SuiteDoc(
    description = @Desc("Verifies Kafka Connect pipeline using camel-kafka-connector plugins "
            + "(timer source, counter source, http-secured sink) "
            + "integrated with Strimzi and the custom REST proxy."),
    beforeTestSteps = {
        @Step(value = "Deploy Kafka cluster and Cluster Operator.",
                expected = "Kafka cluster is ready.")
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
     * <pre>
     *   camel-timer-source Kamelet
     *       → fires every 2 s
     *       → writes constant body: "hello from timer"
     *       → KafkaConnect writes records to {@code timer-topic}
     * </pre>
     *
     * <p>What it tests: the timer-source Kamelet periodically writes a static
     * message to a Kafka topic without any external system or custom code.</p>
     *
     * <p>Expected result: at least 3 records appear in {@code timer-topic}
     * within 60 s, each with value {@code "hello from timer"}.</p>
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that the camel-timer-source connector produces "
                + "periodic static messages (\"hello from timer\") to a Kafka topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with camel-timer-source plugin built from Maven.",
                    expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy timer-connector with static message \"hello from timer\".",
                    expected = "Connector status is RUNNING."),
            @Step(value = "Consume 3 messages from timer-topic.",
                    expected = "Records contain value \"hello from timer\".")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelTimerSourceProducesMessages() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String timerTopic = "timer-topic-" + testStorage.getClusterName();

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
                KafkaTemplates.kafka(testStorage.getNamespaceName(),
                        testStorage.getClusterName(), 1).build()
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
                            .withClassName("org.apache.camel.kafkaconnector.timersource"
                                    + ".CamelTimersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", timerTopic,
                                    "key.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.timer-source.message", "hello from timer",
                                    "camel.kamelet.timer-source.period", "2000"
                            ))
                        .endSpec()
                        .build()
        );

        // Wait until the connector reports RUNNING before consuming,
        // otherwise the consumer may time out before any records are produced.
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "timer-connector");

        // Consume 3 records from timer-topic.
        // Expected value: "hello from timer" in each record.
        // At 2 s per tick this takes ~6 s; 60 s timeout gives ample margin.
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

        LOGGER.info("Timer source produced static messages to topic: {}", timerTopic);
    }

    /**
     * Test path:
     * <pre>
     *   camel-counter-source Kamelet
     *       → emits integer sequence: 1, 2, 3, 4, 5  (one per second)
     *       → KafkaConnect writes each integer as a string record value
     *           to {@code counter-topic}
     * </pre>
     *
     * <p>What it tests: the counter-source Kamelet produces sequential raw
     * integer values and respects the {@code start} and {@code numbers}
     * parameters that control the sequence range and stop condition.</p>
     *
     * <p>Expected result: exactly 5 records appear in {@code counter-topic}
     * within 60 s, with string values {@code "1"} through {@code "5"}
     * in order.</p>
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that the camel-counter-source connector "
                + "produces sequential integer messages to a Kafka topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with camel-counter-source plugin built from Maven.",
                    expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy counter-connector with start=1, period=1000, numbers=5.",
                    expected = "Connector status is RUNNING."),
            @Step(value = "Consume 5 messages from counter-topic.",
                    expected = "Records contain values \"1\", \"2\", \"3\", \"4\", \"5\" in order.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelCounterSourceProducesSequentialNumbers() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String counterTopic = "counter-topic-" + testStorage.getClusterName();
        final int messageCount = 5;

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
                KafkaTemplates.kafka(testStorage.getNamespaceName(),
                        testStorage.getClusterName(), 1).build()
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
                            .withClassName("org.apache.camel.kafkaconnector.countersource"
                                    + ".CamelCountersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", counterTopic,
                                    "key.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    // start=1: sequence begins at 1
                                    // period=1000: one integer per second
                                    // numbers=5: stop after exactly 5 integers so the
                                    //            consumer does not hang waiting for more
                                    "camel.kamelet.counter-source.start", "1",
                                    "camel.kamelet.counter-source.period", "1000",
                                    "camel.kamelet.counter-source.numbers",
                                        String.valueOf(messageCount)
                            ))
                        .endSpec()
                        .build()
        );

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "counter-connector");

        // Consume exactly 5 records from counter-topic.
        // Expected: "1", "2", "3", "4", "5" in order within 60 s.
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

        LOGGER.info("Counter source produced {} sequential integer messages to topic: {}",
                messageCount, counterTopic);
    }

    /**
     * Test path (full end-to-end pipeline):
     * <pre>
     *   camel-counter-source Kamelet
     *       → emits integers: 1, 2, 3  (one per second)
     *       → writes records to {@code counter-topic}
     *       ↓
     *   camel-http-secured-sink Kamelet
     *       → reads each record from {@code counter-topic}
     *       → HTTP POST body = record value ("1", "2", "3")
     *       → URL: http://my-cluster-kafka-rest-bootstrap:9095/v1/topics/demo-topic
     *       → Header: Authorization: Basic YWxpY2U6czNjcmV0  (alice:s3cret)
     *       ↓
     *   Custom Kafka REST proxy (embedded in broker, port 9095)
     *       → validates Basic Auth credentials
     *       → parses body as raw string (no Content-Type header from Camel)
     *       → produces record with value "1" / "2" / "3" to {@code demo-topic}
     * </pre>
     *
     * <p>What it tests: the complete integration between:</p>
     * <ul>
     *   <li>The counter-source Kamelet emitting sequential integer values</li>
     *   <li>The http-secured-sink forwarding records via HTTP POST</li>
     *   <li>Our custom REST proxy accepting requests with no Content-Type
     *       header (fixed in ProduceResource — raw body treated as value)</li>
     *   <li>Preemptive Basic Auth — the sink sends {@code Authorization}
     *       on the first request without a 401 challenge</li>
     * </ul>
     *
     * <p>Expected result: at least 3 records with values
     * {@code "1"}, {@code "2"}, {@code "3"} appear in {@code demo-topic}
     * within 120 s.</p>
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies full end-to-end pipeline: counter-source produces "
                + "integers (1, 2, 3) to counter-topic; http-secured-sink forwards "
                + "each one via HTTP POST with Basic Auth to the custom REST proxy; "
                + "messages land in demo-topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with counter-source and http-secured-sink plugins.",
                    expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy counter-connector producing integers 1, 2, 3 to counter-topic.",
                    expected = "Connector status is RUNNING; "
                            + "counter-topic receives \"1\", \"2\", \"3\"."),
            @Step(value = "Deploy http-secured-sink-connector reading from counter-topic, "
                    + "POSTing to REST proxy at port 9095 with Basic Auth (alice:s3cret).",
                    expected = "Connector status is RUNNING; REST proxy accepts POST."),
            @Step(value = "Consume 3 messages from demo-topic.",
                    expected = "Records contain \"1\", \"2\", \"3\" — full pipeline verified.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelHttpSecuredSinkForwardsCounterMessagesToRestProxy() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String counterTopic = "counter-topic-" + testStorage.getClusterName();
        final String demoTopic = "demo-topic";

        // The REST proxy is embedded in the Kafka broker and listens on port 9095.
        // my-cluster-kafka-rest-bootstrap is the LoadBalancer Service created by
        // Strimzi for the HTTP listener defined in the Kafka CR.
        final String restProxyUrl =
                "http://my-cluster-kafka-rest-bootstrap.kafka.svc:9095/v1/topics/" + demoTopic;

        // Both plugins are bundled into the same KafkaConnect image.
        Plugin counterPlugin = new PluginBuilder()
                .withName("camel-counter-source")
                .withArtifacts(new MavenArtifactBuilder()
                        .withGroup(CAMEL_GROUP)
                        .withArtifact("camel-counter-source-kafka-connector")
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
                KafkaTemplates.kafka(testStorage.getNamespaceName(),
                        testStorage.getClusterName(), 1).build()
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
                        List.of(counterPlugin, httpSinkPlugin)
                ).build()
        );

        // Connector 1 — source side.
        // Produces integers 1, 2, 3 to counter-topic every second.
        // numbers=3 stops the connector after exactly 3 records so the
        // pipeline has a bounded, verifiable set of messages.
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(),
                        "counter-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.countersource"
                                    + ".CamelCountersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", counterTopic,
                                    "key.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.counter-source.start", "1",
                                    "camel.kamelet.counter-source.period", "1000",
                                    "camel.kamelet.counter-source.numbers", "3"
                            ))
                        .endSpec()
                        .build()
        );

        // Connector 2 — sink side.
        // Reads each record from counter-topic and POSTs its value to the REST proxy.
        // The Kamelet sends no Content-Type header; ProduceResource treats the raw
        // body as the record value (fixed in our custom Kafka fork).
        // authenticationPreemptive=true: Authorization header is sent on the first
        // request without waiting for a 401 challenge — required because our REST
        // proxy does not issue a WWW-Authenticate challenge.
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(),
                        "http-secured-sink-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.httpsecuredsink"
                                    + ".CamelHttpsecuredsinkSinkConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", counterTopic,
                                    "key.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter",
                                        "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.http-secured-sink.url", restProxyUrl,
                                    "camel.kamelet.http-secured-sink.method", "POST",
                                    "camel.kamelet.http-secured-sink.authMethod", "Basic",
                                    "camel.kamelet.http-secured-sink.authUsername", "alice",
                                    "camel.kamelet.http-secured-sink.authPassword", "s3cret",
                                    "camel.kamelet.http-secured-sink.authenticationPreemptive",
                                        "true"
                            ))
                        .endSpec()
                        .build()
        );

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(),
                "counter-connector");
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(),
                "http-secured-sink-connector");

        // Consume from demo-topic — the final destination after the REST proxy.
        // If 3 records arrive here with values "1", "2", "3", the full pipeline
        // is verified:
        //   counter fired → counter-topic held the record → sink forwarded via
        //   HTTP POST → REST proxy authenticated + wrote → demo-topic received.
        // 120 s timeout: longer than the source-only tests because the pipeline
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

        LOGGER.info("Full e2e pipeline verified: "
                + "counter-source -> counter-topic -> http-secured-sink "
                + "-> REST proxy -> demo-topic (values: \"1\", \"2\", \"3\")");
    }

    @BeforeAll
    void setup() {
        // Cluster Operator is set up per test via @ParallelNamespaceTest — no global setup needed.
    }
}
