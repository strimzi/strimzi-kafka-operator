/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.vertx.core.json.JsonArray;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(INTERNAL_CLIENTS_USED)
class HttpBridgeST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);
    private static final String NAMESPACE = "bridge-cluster-test";

    @Test
    void testSendSimpleMessage() {
        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaClientsResource.producerStrimziBridge(producerName, bridgeServiceName, bridgePort, TOPIC_NAME, MESSAGE_COUNT).done();
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), is(MESSAGE_COUNT));

        // Checking labels for Kafka Bridge
        verifyLabelsOnPods(CLUSTER_NAME, "my-bridge", null, "KafkaBridge");
        verifyLabelsForService(CLUSTER_NAME, "my-bridge", "KafkaBridge");
    }

    @Test
    void testReceiveSimpleMessage() {
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaClientsResource.consumerStrimziBridge(consumerName, bridgeServiceName, bridgePort, TOPIC_NAME, MESSAGE_COUNT).done();

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .build();

        assertThat(internalKafkaClient.sendMessagesPlain(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
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

        Map<String, String> bridgeSnapshot = DeploymentUtils.depSnapshot(KafkaBridgeResources.deploymentName(bridgeName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable),
                is(not("test.value"))
        );

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

        DeploymentUtils.waitTillDepHasRolled(KafkaBridgeResources.deploymentName(bridgeName), 1, bridgeSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarUpdated);
        checkComponentConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_PRODUCER_CONFIG", producerConfig);
        checkComponentConfiguration(KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_CONSUMER_CONFIG", consumerConfig);
    }

    @Test
    void testDiscoveryAnnotation() {
        Service bridgeService = kubeClient().getService(KafkaBridgeResources.serviceName(CLUSTER_NAME));
        String bridgeServiceDiscoveryAnnotation = bridgeService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(bridgeServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(8080, "http", "none")));
    }

    @Test
    void testScaleBridgeToZero() {
        String bridgeName = "scaling-bridge";

        KafkaBridgeResource.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();

        List<String> bridgePods = kubeClient().listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        String deploymentName = KafkaBridgeResources.deploymentName(bridgeName);

        assertThat(bridgePods.size(), is(1));

        LOGGER.info("Scaling KafkaBridge to zero replicas");
        KafkaBridgeResource.replaceBridgeResource(bridgeName, kafkaBridge -> kafkaBridge.getSpec().setReplicas(0));

        KafkaBridgeUtils.waitForKafkaBridgeReady(CLUSTER_NAME);
        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(deploymentName), 0, true);

        bridgePods = kubeClient().listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        KafkaBridgeStatus bridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getStatus();

        assertThat(bridgePods.size(), is(0));
        assertThat(bridgeStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @Test
    void testScaleBridgeSubresource() {
        String bridgeName = "scaling-bridge";

        KafkaBridgeResource.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();

        int scaleTo = 4;
        long bridgeObsGen = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getStatus().getObservedGeneration();
        String bridgeGenName = kubeClient().listPodsByPrefixInName(bridgeName).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaBridge subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaBridge.RESOURCE_KIND, bridgeName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaBridgeResources.deploymentName(bridgeName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> bridgePods = kubeClient().listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        assertThat(bridgePods.size(), is(4));
        assertThat(KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(bridgeObsGen < KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getStatus().getObservedGeneration(), is(true));
        for (String pod : bridgePods) {
            assertThat(pod.contains(bridgeGenName), is(true));
        }
    }

    @BeforeAll
    void createClassResources() throws Exception {
        deployClusterOperator(NAMESPACE);
        LOGGER.info("Deploy Kafka and KafkaBridge before tests");
        // Deploy kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .done();
    }
}
