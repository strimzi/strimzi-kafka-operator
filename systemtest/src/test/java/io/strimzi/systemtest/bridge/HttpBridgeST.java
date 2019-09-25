/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.template.ContainerTemplateBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.HttpBridgeBaseST;
import io.strimzi.systemtest.utils.StUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(BRIDGE)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@ExtendWith(VertxExtension.class)
class HttpBridgeST extends HttpBridgeBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);

    public static final String NAMESPACE = "bridge-cluster-test";
    private String bridgeHost = "";
    private int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;

    @Test
    void testSendSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-send";
        // Create topic
        testClassResources().topic(CLUSTER_NAME, topicName).done();

        JsonObject records = generateHttpMessages(messageCount);
        JsonObject response = sendHttpRequests(records, bridgeHost, bridgePort, topicName);
        checkSendResponse(response, messageCount);
        receiveMessagesExternal(NAMESPACE, topicName, messageCount);

        // Checking labels for Kafka Bridge
        verifyLabelsOnPods(CLUSTER_NAME, "my-bridge", null, "KafkaBridge");
        verifyLabelsForService(CLUSTER_NAME, "my-bridge", "KafkaBridge");
    }

    @Test
    void testReceiveSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-receive";
        // Create topic
        testClassResources().topic(CLUSTER_NAME, topicName).done();

        String name = "my-kafka-consumer";
        String groupId = "my-group-" + new Random().nextInt(Integer.MAX_VALUE);

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create consumer
        JsonObject response = createBridgeConsumer(config, bridgeHost, bridgePort, groupId);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(name));
        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(topicName);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);
        // Subscribe
        assertTrue(subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, name));
        // Send messages to Kafka
        sendMessagesExternal(NAMESPACE, topicName, messageCount);
        // Try to consume messages
        JsonArray bridgeResponse = receiveHttpRequests(bridgeHost, bridgePort, groupId, name);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = receiveHttpRequests(bridgeHost, bridgePort, groupId, name);
        }
        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(messageCount));
        // Delete consumer
        assertTrue(deleteConsumer(bridgeHost, bridgePort, groupId, name));
    }

    @Test
    void testCustomAndUpdatedValues() {
        createTestMethodResources();
        String bridgeName = "custom-bridge";
        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("acks", "1");

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "earliest");

        int initialDelaySeconds = 30;
        int timeoutSeconds = 10;
        int updatedInitialDelaySeconds = 31;
        int updatedTimeoutSeconds = 11;
        int periodSeconds = 10;
        int successThreshold = 1;
        int failureThreshold = 3;
        int updatedPeriodSeconds = 5;
        int updatedFailureThreshold = 1;

        testMethodResources().kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, 8080)
                .editSpec()
                    .withNewTemplate()
                        .withBridgeContainer(
                                new ContainerTemplateBuilder().withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral)).build()
                        )
                    .endTemplate()
                    .withNewProducer()
                    .endProducer()
                    .withNewConsumer()
                    .endConsumer()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                        .withPeriodSeconds(periodSeconds)
                        .withSuccessThreshold(successThreshold)
                        .withFailureThreshold(failureThreshold)
                    .endReadinessProbe()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                        .withPeriodSeconds(periodSeconds)
                        .withSuccessThreshold(successThreshold)
                        .withFailureThreshold(failureThreshold)
                    .endLivenessProbe()
                .endSpec().done();

        Map<String, String> connectSnapshot = StUtils.depSnapshot(KafkaBridgeResources.deploymentName(bridgeName));

        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkContainerConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarGeneral);

        replaceBridgeResource(bridgeName, kb -> {
            kb.getSpec().getTemplate().getBridgeContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kb.getSpec().getProducer().setConfig(producerConfig);
            kb.getSpec().getConsumer().setConfig(consumerConfig);
            kb.getSpec().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kb.getSpec().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kb.getSpec().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kb.getSpec().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kb.getSpec().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kb.getSpec().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kb.getSpec().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kb.getSpec().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
        });

        StUtils.waitTillDepHasRolled(KafkaBridgeResources.deploymentName(bridgeName), 1, connectSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(KafkaBridgeResources.deploymentName(CLUSTER_NAME), KafkaBridgeResources.deploymentName(CLUSTER_NAME), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkContainerConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarUpdated);
        checkContainerConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_PRODUCER_CONFIG", producerConfig);
        checkContainerConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_CONSUMER_CONFIG", consumerConfig);

        deleteTestMethodResources();
    }

    @BeforeAll
    void createClassResources() throws InterruptedException {
        LOGGER.info("Deploy Kafka and Kafka Bridge before tests");
        // Deploy kafka
        testClassResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalNodePort()
                .withTls(false)
                .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
                .endSpec().done();

        // Deploy http bridge
        testClassResources().kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT).done();

        deployBridgeNodePortService();
        bridgePort = getBridgeNodePort();
        bridgeHost = kubeClient(NAMESPACE).getNodeAddress();
    }

    @Override
    public String getBridgeNamespace() {
        return NAMESPACE;
    }
}
