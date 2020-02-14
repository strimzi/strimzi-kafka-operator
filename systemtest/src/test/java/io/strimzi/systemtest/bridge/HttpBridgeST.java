/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
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
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        JsonObject records = HttpUtils.generateHttpMessages(messageCount);
        JsonObject response = HttpUtils.sendMessagesHttpRequest(records, bridgeHost, bridgePort, topicName, client);
        KafkaBridgeUtils.checkSendResponse(response, messageCount);

        Future<Integer> consumer = kafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, messageCount);
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(messageCount));

        // Checking labels for Kafka Bridge
        verifyLabelsOnPods(CLUSTER_NAME, "my-bridge", null, "KafkaBridge");
        verifyLabelsForService(CLUSTER_NAME, "my-bridge", "KafkaBridge");
    }

    @Test
    void testReceiveSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-receive";
        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

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
        assertThat(HttpUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, name, client), is(true));
        // Send messages to Kafka
        kafkaClient.sendMessages(CLUSTER_NAME, NAMESPACE, topicName, messageCount);
        // Try to consume messages
        JsonArray bridgeResponse = HttpUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, groupId, name, client);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = HttpUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, groupId, name, client);
        }
        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(messageCount));
        // Delete consumer
        assertThat(deleteConsumer(bridgeHost, bridgePort, groupId, name), is(true));
    }

    @Test
    void testCustomAndUpdatedValues() {
        String bridgeName = "custom-bridge";
        String usedVariable = "KAFKA_BRIDGE_PRODUCER_CONFIG";
        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");
        envVarGeneral.put(usedVariable, "test.value");

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

        KafkaBridgeResource.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewTemplate()
                    .withNewBridgeContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endBridgeContainer()
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

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(KafkaBridgeResources.deploymentName(bridgeName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarGeneral);

        StUtils.checkCologForUsedVariable(usedVariable);

        LOGGER.info("Updating values in Bridge container");
        KafkaBridgeResource.replaceBridgeResource(bridgeName, kb -> {
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

        DeploymentUtils.waitTillDepHasRolled(KafkaBridgeResources.deploymentName(bridgeName), 1, connectSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarUpdated);
        checkComponentConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_PRODUCER_CONFIG", producerConfig);
        checkComponentConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_CONSUMER_CONFIG", consumerConfig);
    }

    @BeforeAll
    void createClassResources() throws InterruptedException {
        LOGGER.info("Deploy Kafka and Kafka Bridge before tests");
        // Deploy kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
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
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();

        Service service = KafkaBridgeUtils.createBridgeNodePortService(CLUSTER_NAME, NAMESPACE, bridgeExternalService);
        KubernetesResource.createServiceResource(service, NAMESPACE).done();
        ServiceUtils.waitForNodePortService(bridgeExternalService);

        bridgePort = KafkaBridgeUtils.getBridgeNodePort(NAMESPACE, bridgeExternalService);
        bridgeHost = kubeClient(NAMESPACE).getNodeAddress();
    }
}
