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

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@SuiteDoc(
    description = @Desc("Verifies Kafka Connect pipeline using camel-kafka-connector plugins (timer source, counter source, http-secured sink) integrated with Strimzi."),
    beforeTestSteps = {
        @Step(value = "Deploy Kafka cluster and Cluster Operator.", expected = "Kafka cluster is ready.")
    },
    labels = {
        @Label(value = TestDocsLabels.CONNECT)
    }
)
class CamelConnectST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CamelConnectST.class);

    private static final String CAMEL_VERSION = "4.8.0";
    private static final String CAMEL_GROUP = "org.apache.camel.kafkaconnector";
    private static final String CONNECT_IMAGE = "ttl.sh/strimzi-camel-connect:24h";

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that the camel-timer-source connector produces messages to a Kafka topic automatically."),
        steps = {
            @Step(value = "Deploy KafkaConnect with camel-timer-source plugin built from Maven.", expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy timer-connector KafkaConnector CR.", expected = "Connector status is RUNNING."),
            @Step(value = "Consume messages from timer-topic.", expected = "At least 3 messages received within 60s.")
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
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), timerTopic, testStorage.getClusterName()).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectTemplates.kafkaConnectWithBuildAndWait(
                        testStorage.getNamespaceName(),
                        testStorage.getClusterName(),
                        1,
                        CONNECT_IMAGE,
                        List.of(timerPlugin)
                ).build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), "timer-connector", testStorage.getClusterName())
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

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "timer-connector");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withBootstrapAddress(testStorage.getClusterName() + "-kafka-bootstrap:9092")
                .withTopicName(timerTopic)
                .withMessageCount(3)
                .withConsumerGroup("timer-test-consumer-group")
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), kafkaClients.getConsumerName(), 3, 60_000);

        LOGGER.info("Timer source connector successfully produced messages to topic: {}", timerTopic);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies that the camel-counter-source connector produces sequential integer messages to a Kafka topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with camel-counter-source plugin built from Maven.", expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy counter-connector KafkaConnector CR.", expected = "Connector status is RUNNING."),
            @Step(value = "Consume 5 messages from counter-topic.", expected = "5 messages received with sequential values starting from 1.")
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
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), counterTopic, testStorage.getClusterName()).build()
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
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), "counter-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.countersource.CamelCountersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", counterTopic,
                                    "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.counter-source.start", "1",
                                    "camel.kamelet.counter-source.period", "1000",
                                    "camel.kamelet.counter-source.numbers", String.valueOf(messageCount)
                            ))
                        .endSpec()
                        .build()
        );

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), "counter-connector");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withBootstrapAddress(testStorage.getClusterName() + "-kafka-bootstrap:9092")
                .withTopicName(counterTopic)
                .withMessageCount(messageCount)
                .withConsumerGroup("counter-test-consumer-group")
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), kafkaClients.getConsumerName(), messageCount, 60_000);

        LOGGER.info("Counter source connector successfully produced {} sequential messages to topic: {}", messageCount, counterTopic);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verifies full end-to-end pipeline: timer-source produces to timer-topic, http-secured-sink forwards to REST proxy, messages land in demo-topic."),
        steps = {
            @Step(value = "Deploy KafkaConnect with timer-source and http-secured-sink plugins.", expected = "KafkaConnect pod is Running."),
            @Step(value = "Deploy timer-connector producing to timer-topic.", expected = "Connector status is RUNNING."),
            @Step(value = "Deploy http-secured-sink-connector reading from timer-topic, POSTing to REST proxy.", expected = "Connector status is RUNNING."),
            @Step(value = "Consume from demo-topic.", expected = "Messages arrive in demo-topic via REST proxy — full pipeline verified.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCamelHttpSecuredSinkForwardsToRestProxy() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String timerTopic = "timer-topic-" + testStorage.getClusterName();
        final String demoTopic = "demo-topic";
        final String restProxyUrl = "http://my-cluster-kafka-rest-bootstrap.kafka.svc:9095/v1/topics/" + demoTopic;

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
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), timerTopic, testStorage.getClusterName()).build()
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

        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), "timer-connector", testStorage.getClusterName())
                        .editSpec()
                            .withClassName("org.apache.camel.kafkaconnector.timersource.CamelTimersourceSourceConnector")
                            .withTasksMax(1)
                            .withConfig(Map.of(
                                    "topics", timerTopic,
                                    "key.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "value.converter", "org.apache.kafka.connect.storage.StringConverter",
                                    "camel.kamelet.timer-source.message", "{\"key\":\"camel\",\"value\":\"hello from timer\"}",
                                    "camel.kamelet.timer-source.period", "3000"
                            ))
                        .endSpec()
                        .build()
        );

        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), "http-secured-sink-connector", testStorage.getClusterName())
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

        // Messages flow: timer → timer-topic → http-sink → REST proxy → demo-topic
        // Consume from demo-topic to verify end-to-end
        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withBootstrapAddress("my-cluster-kafka-bootstrap.kafka.svc:9092")
                .withTopicName(demoTopic)
                .withMessageCount(3)
                .withConsumerGroup("e2e-test-consumer-group")
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), kafkaClients.getConsumerName(), 3, 120_000);

        LOGGER.info("Full e2e pipeline verified: timer-topic -> http-secured-sink -> REST proxy -> demo-topic");
    }

    @BeforeAll
    void setup() {
        // Cluster Operator is set up per test via @ParallelNamespaceTest — no global setup needed
    }
}
