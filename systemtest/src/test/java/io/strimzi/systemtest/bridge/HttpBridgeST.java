/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.parallel.ParallelSuiteController;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
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
import org.junit.jupiter.api.AfterAll;
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

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
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
class HttpBridgeST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);
    private static final String NAMESPACE = "bridge-namespace";

    private final String httpBridgeClusterName = "http-bridge-cluster-name";
    private final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
    private final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

    private String kafkaClientsPodName;

    @ParallelTest
    void testSendSimpleMessage(ExtensionContext extensionContext) {
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);

        final KafkaBridgeExampleClients kafkaBridgeClientJob = (KafkaBridgeExampleClients) new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(NAMESPACE)
            .build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeClusterName, topicName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());

        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), is(MESSAGE_COUNT));

        // Checking labels for Kafka Bridge
        verifyLabelsOnPods(NAMESPACE, httpBridgeClusterName, "my-bridge", "KafkaBridge");
        verifyLabelsForService(NAMESPACE, httpBridgeClusterName, "my-bridge", "KafkaBridge");
    }

    @ParallelTest
    void testReceiveSimpleMessage(ExtensionContext extensionContext) {
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeClusterName, topicName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        final KafkaBridgeExampleClients kafkaBridgeClientJob = (KafkaBridgeExampleClients) new KafkaBridgeExampleClients.Builder()
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(NAMESPACE)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge().build());

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.sendMessagesPlain(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
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

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
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

        Map<String, String> bridgeSnapshot = DeploymentUtils.depSnapshot(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(NAMESPACE, kubeClient(NAMESPACE).listPods(Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable),
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
        }, NAMESPACE);

        DeploymentUtils.waitTillDepHasRolled(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), 1, bridgeSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), envVarUpdated);
        checkComponentConfiguration(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_PRODUCER_CONFIG", producerConfig);
        checkComponentConfiguration(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), KafkaBridgeResources.deploymentName(bridgeName), "KAFKA_BRIDGE_CONSUMER_CONFIG", consumerConfig);
    }

    @ParallelTest
    void testDiscoveryAnnotation() {
        Service bridgeService = kubeClient(NAMESPACE).getService(NAMESPACE, KafkaBridgeResources.serviceName(httpBridgeClusterName));
        String bridgeServiceDiscoveryAnnotation = bridgeService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(bridgeServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(8080, "http", "none", false)));
    }

    @ParallelTest
    void testScaleBridgeToZero(ExtensionContext extensionContext) {

        String bridgeName = "scaling-bridge-down";

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        List<String> bridgePods = kubeClient(NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        String deploymentName = KafkaBridgeResources.deploymentName(bridgeName);

        assertThat(bridgePods.size(), is(1));

        LOGGER.info("Scaling KafkaBridge to zero replicas");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, kafkaBridge -> kafkaBridge.getSpec().setReplicas(0), NAMESPACE);

        KafkaBridgeUtils.waitForKafkaBridgeReady(NAMESPACE, httpBridgeClusterName);
        PodUtils.waitForPodsReady(kubeClient(NAMESPACE).getDeploymentSelectors(deploymentName), 0, true);

        bridgePods = kubeClient(NAMESPACE).listPodNames(NAMESPACE, httpBridgeClusterName, Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        KafkaBridgeStatus bridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getStatus();

        assertThat(bridgePods.size(), is(0));
        assertThat(bridgeStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelTest
    void testScaleBridgeSubresource(ExtensionContext extensionContext) {
        String bridgeName = "scaling-bridge-up";

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        int scaleTo = 4;
        long bridgeObsGen = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get().getStatus().getObservedGeneration();
        String bridgeGenName = kubeClient(NAMESPACE).listPodsByPrefixInName(bridgeName).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaBridge subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient(NAMESPACE).scaleByName(KafkaBridge.RESOURCE_KIND, bridgeName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(NAMESPACE, KafkaBridgeResources.deploymentName(bridgeName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> bridgePods = kubeClient(NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
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

    @ParallelTest
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        String bridgeName = "example-bridge";

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String bridgeDepName = KafkaBridgeResources.deploymentName(bridgeName);

        LOGGER.info("Adding label to KafkaBridge resource, the CR should be recreated");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName,
            kb -> kb.getMetadata().setLabels(Collections.singletonMap("some", "label")), NAMESPACE);
        DeploymentUtils.waitForDeploymentAndPodsReady(NAMESPACE, bridgeDepName, 1);

        KafkaBridge kafkaBridge = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaBridge.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaBridge.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaBridge.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName,
            kb -> kb.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), NAMESPACE);
        KafkaBridgeUtils.waitForKafkaBridgeReady(NAMESPACE, bridgeName);

        LOGGER.info("Adding another label to KafkaBridge resource, pods should be rolled");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, kb -> kb.getMetadata().getLabels().put("another", "label"), NAMESPACE);
        DeploymentUtils.waitForDeploymentAndPodsReady(NAMESPACE, bridgeDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kafkaBridge = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(bridgeName).get();
        assertThat(kafkaBridge.getStatus().getObservedGeneration(), is(2L));
        assertThat(kafkaBridge.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kafkaBridge.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    @ParallelTest
    void testCustomBridgeLabelsAreProperlySet(ExtensionContext extensionContext) {
        final String bridgeName = "bridge-" + mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        // get service with custom labels
        final Service kafkaBridgeService = kubeClient(NAMESPACE).getService(NAMESPACE, KafkaBridgeResources.serviceName(bridgeName));

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
    void createClassResources(ExtensionContext extensionContext) throws InterruptedException {
        // active waiting with sleep
        while (ParallelSuiteController.waitUntilZeroParallelSuites());

        cluster.createNamespace(extensionContext, NAMESPACE);

        // un-install old cluster operator
        install = BeforeAllOnce.install;
        install.unInstall();
        // install new one with branch new configuration
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.sharedExtensionContext)
            .withNamespace(Constants.INFRA_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
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
                )
            ).createInstallation()
            .runInstallation();

        LOGGER.info("Deploy Kafka and KafkaBridge before tests");
        String kafkaClientsName = NAMESPACE + "-shared-" + Constants.KAFKA_CLIENTS;

        // Deploy kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeClusterName, 1, 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        kafkaClientsPodName = kubeClient(NAMESPACE).listPodsByPrefixInName(NAMESPACE, kafkaClientsName).get(0).getMetadata().getName();

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(httpBridgeClusterName,
            KafkaResources.plainBootstrapAddress(httpBridgeClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());
    }

    @AfterAll
    void tearDown(ExtensionContext extensionContext) throws Exception {
        this.afterAllMayOverride(extensionContext);

        install = install.rollbackToDefaultConfiguration();
    }
}
