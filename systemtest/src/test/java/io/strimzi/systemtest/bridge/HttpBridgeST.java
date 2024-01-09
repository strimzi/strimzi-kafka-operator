/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
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
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(INTERNAL_CLIENTS_USED)
class HttpBridgeST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);

    private final String httpBridgeClusterName = "http-bridge-cluster-name";

    private String kafkaClientsPodName;

    @ParallelTest
    void testSendSimpleMessage(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String topicName = testStorage.getTopicName();
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        final BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        // Create topic
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(httpBridgeClusterName, topicName, Environment.TEST_SUITE_NAMESPACE).build());

        resourceManager.createResourceWithWait(extensionContext, kafkaBridgeClientJob.producerStrimziBridge());

        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, MESSAGE_COUNT);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(httpBridgeClusterName))
            .withConsumerName(consumerName)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.consumerStrimzi());

        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, MESSAGE_COUNT);

        // Checking labels for KafkaBridge
        verifyLabelsOnPods(Environment.TEST_SUITE_NAMESPACE, httpBridgeClusterName, "my-bridge", "KafkaBridge");
        verifyLabelsForService(Environment.TEST_SUITE_NAMESPACE, httpBridgeClusterName, "bridge", "bridge-service", "KafkaBridge");
    }

    @ParallelTest
    void testReceiveSimpleMessage(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String topicName = testStorage.getTopicName();
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(httpBridgeClusterName, topicName, Environment.TEST_SUITE_NAMESPACE).build());

        final BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge());

        // Send messages to Kafka
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(httpBridgeClusterName))
            .withProducerName(producerName)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerStrimzi());

        ClientUtils.waitForClientsSuccess(producerName, consumerName, Environment.TEST_SUITE_NAMESPACE, MESSAGE_COUNT);
    }

    @ParallelTest
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {

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

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
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
            .endSpec()
            .build());

        Map<String, String> bridgeSnapshot = DeploymentUtils.depSnapshot(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verifying values before update");
        checkReadinessLivenessProbe(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(Environment.TEST_SUITE_NAMESPACE, kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, httpBridgeClusterName, Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable),
                is(not("test.value"))
        );

        LOGGER.info("Updating values in Bridge container");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, kb -> {
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
        }, Environment.TEST_SUITE_NAMESPACE);

        DeploymentUtils.waitTillDepHasRolled(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), 1, bridgeSnapshot);

        LOGGER.info("Verifying values after update");
        checkReadinessLivenessProbe(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), envVarUpdated);
        checkComponentConfiguration(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), "KAFKA_BRIDGE_PRODUCER_CONFIG", producerConfig);
        checkComponentConfiguration(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), "KAFKA_BRIDGE_CONSUMER_CONFIG", consumerConfig);
    }

    @ParallelTest
    void testDiscoveryAnnotation() {
        Service bridgeService = kubeClient().getService(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.serviceName(httpBridgeClusterName));
        String bridgeServiceDiscoveryAnnotation = bridgeService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(bridgeServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(8080, "http", "none", false)));
    }

    @ParallelTest
    void testScaleBridgeToZero(ExtensionContext extensionContext) {

        String bridgeName = "scaling-bridge-down";

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        List<String> bridgePods = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        String deploymentName = KafkaBridgeResources.componentName(bridgeName);

        assertThat(bridgePods.size(), is(1));

        LOGGER.info("Scaling KafkaBridge to zero replicas");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, kafkaBridge -> kafkaBridge.getSpec().setReplicas(0), Environment.TEST_SUITE_NAMESPACE);

        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, httpBridgeClusterName);
        PodUtils.waitForPodsReady(Environment.TEST_SUITE_NAMESPACE, kubeClient().getDeploymentSelectors(Environment.TEST_SUITE_NAMESPACE, deploymentName), 0, true);

        bridgePods = kubeClient().listPodNames(Environment.TEST_SUITE_NAMESPACE, httpBridgeClusterName, Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        KafkaBridgeStatus bridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus();

        assertThat(bridgePods.size(), is(0));
        assertThat(bridgeStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelTest
    void testScaleBridgeSubresource(ExtensionContext extensionContext) {
        String bridgeName = "scaling-bridge-up";

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        int scaleTo = 4;
        long bridgeObsGen = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus().getObservedGeneration();
        String bridgeGenName = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodsByPrefixInName(bridgeName).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaBridge subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).scaleByName(KafkaBridge.RESOURCE_KIND, bridgeName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        StUtils.waitUntilSupplierIsSatisfied(
            () -> {
                List<String> bridgePods = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);

                return bridgePods.size() == 4 &&
                    KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getSpec().getReplicas() == 4 &&
                    KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus().getReplicas() == 4 &&
                    /*
                    observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
                    the observed generation is increased
                    */
                    bridgeObsGen < KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus().getObservedGeneration();
            });

        for (final String pod : kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName)) {
            assertThat(pod.contains(bridgeGenName), is(true));
        }
    }

    @ParallelTest
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        String bridgeName = "example-bridge";

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String bridgeDepName = KafkaBridgeResources.componentName(bridgeName);

        LOGGER.info("Adding label to KafkaBridge resource, the CR should be recreated");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName,
            kb -> kb.getMetadata().setLabels(Collections.singletonMap("some", "label")), Environment.TEST_SUITE_NAMESPACE);
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1);

        KafkaBridge kafkaBridge = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaBridge.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaBridge.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaBridge.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing Deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName,
            kb -> kb.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), Environment.TEST_SUITE_NAMESPACE);
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, bridgeName);

        LOGGER.info("Adding another label to KafkaBridge resource, Pods should be rolled");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, kb -> kb.getMetadata().getLabels().put("another", "label"), Environment.TEST_SUITE_NAMESPACE);
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");

        StUtils.waitUntilSupplierIsSatisfied(() -> {
            final KafkaBridge kB = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get();

            return kB.getStatus().getObservedGeneration() == 2L &&
                kB.getMetadata().getLabels().toString().contains("another=label") &&
                kB.getSpec().getTemplate().getDeployment().getDeploymentStrategy().equals(DeploymentStrategy.ROLLING_UPDATE);
        });
    }

    @ParallelTest
    void testCustomBridgeLabelsAreProperlySet(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String bridgeName = "bridge-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        // get service with custom labels
        final Service kafkaBridgeService = kubeClient().getService(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.serviceName(bridgeName));

        // filter only app-bar service
        final Map<String, String> filteredActualKafkaBridgeCustomLabels =
            kafkaBridgeService.getMetadata().getLabels().entrySet().stream()
                .filter(item -> item.getKey().equals("app") && item.getValue().equals("bar"))
                .collect(Collectors.toMap(item -> item.getKey(), item -> item.getValue()));

        final Map<String, String> filteredActualKafkaBridgeCustomAnnotations =
            kafkaBridgeService.getMetadata().getAnnotations().entrySet().stream()
                .filter(item -> item.getKey().equals("bar") && item.getValue().equals("app"))
                .collect(Collectors.toMap(item -> item.getKey(), item -> item.getValue()));

        // verify phase: that inside KafkaBridge we can find 'exceptedKafkaBridgeCustomLabels' and 'exceptedKafkaBridgeCustomAnnotations' previously defined
        assertThat(filteredActualKafkaBridgeCustomLabels.size(), is(Collections.singletonMap("app", "bar").size()));
        assertThat(filteredActualKafkaBridgeCustomAnnotations.size(), is(Collections.singletonMap("bar", "app").size()));
        assertThat(filteredActualKafkaBridgeCustomLabels, is(Collections.singletonMap("app", "bar")));
        assertThat(filteredActualKafkaBridgeCustomAnnotations, is(Collections.singletonMap("bar", "app")));
    }

    @BeforeAll
    void createClassResources(ExtensionContext extensionContext) {
        final String namespaceToWatch = Environment.isNamespaceRbacScope() ? CO_NAMESPACE : TestConstants.WATCH_ALL_NAMESPACES;

        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
                .withNamespace(CO_NAMESPACE)
                .withWatchingNamespaces(namespaceToWatch)
                .withExtraEnvVars(
                    Arrays.asList(
                        new EnvVarBuilder()
                                .withName("STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_LABELS")
                                .withValue("app=bar")
                                .build(),
                        new EnvVarBuilder()
                                .withName("STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_ANNOTATIONS")
                                .withValue("bar=app")
                                .build()
                    ))
                .createInstallation()
                .runInstallation();

        LOGGER.info("Deploying Kafka and KafkaBridge before tests");

        // Deploy kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeClusterName, 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        // Deploy http bridge
        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(httpBridgeClusterName,
            KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());
    }
}
