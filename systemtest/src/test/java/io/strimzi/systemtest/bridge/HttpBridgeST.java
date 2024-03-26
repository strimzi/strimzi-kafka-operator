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
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.vertx.core.json.JsonArray;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private TestStorage suiteTestStorage;

    @ParallelTest
    void testSendSimpleMessage() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()))
            .withComponentName(KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        // Create topic
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(suiteTestStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        // Checking labels for KafkaBridge
        VerificationUtils.verifyPodsLabels(testStorage.getNamespaceName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()), KafkaBridgeResource.getLabelSelector(suiteTestStorage.getClusterName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName())));
        VerificationUtils.verifyServiceLabels(testStorage.getNamespaceName(), KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()), KafkaBridgeResource.getLabelSelector(suiteTestStorage.getClusterName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName())));
    }

    @ParallelTest
    void testReceiveSimpleMessage() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(suiteTestStorage.getClusterName(), testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        final BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()))
            .withComponentName(KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        // Start receiving messages with bridge
        resourceManager.createResourceWithWait(kafkaBridgeClientJob.consumerStrimziBridge());

        // Send messages to Kafka
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi());

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    @ParallelTest
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

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
        VerificationUtils.verifyReadinessAndLivenessProbes(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        VerificationUtils.verifyContainerEnvVariables(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(Environment.TEST_SUITE_NAMESPACE, kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable),
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
        VerificationUtils.verifyReadinessAndLivenessProbes(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        VerificationUtils.verifyContainerEnvVariables(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), envVarUpdated);
        VerificationUtils.verifyComponentConfiguration(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), "KAFKA_BRIDGE_PRODUCER_CONFIG", producerConfig);
        VerificationUtils.verifyComponentConfiguration(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), "KAFKA_BRIDGE_CONSUMER_CONFIG", consumerConfig);
    }

    @ParallelTest
    void testDiscoveryAnnotation() {
        Service bridgeService = kubeClient().getService(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()));
        String bridgeServiceDiscoveryAnnotation = bridgeService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(bridgeServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(8080, "http", "none", false)));
    }

    @ParallelTest
    void testScaleBridgeToZero() {

        String bridgeName = "scaling-bridge-down";

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        List<String> bridgePods = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        String deploymentName = KafkaBridgeResources.componentName(bridgeName);

        assertThat(bridgePods.size(), is(1));

        LOGGER.info("Scaling KafkaBridge to zero replicas");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, kafkaBridge -> kafkaBridge.getSpec().setReplicas(0), Environment.TEST_SUITE_NAMESPACE);

        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName());
        PodUtils.waitForPodsReady(Environment.TEST_SUITE_NAMESPACE, kubeClient().getDeploymentSelectors(Environment.TEST_SUITE_NAMESPACE, deploymentName), 0, true);

        bridgePods = kubeClient().listPodNames(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        KafkaBridgeStatus bridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus();

        assertThat(bridgePods.size(), is(0));
        assertThat(bridgeStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelTest
    void testScaleBridgeSubresource() {
        String bridgeName = "scaling-bridge-up";

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
    void testConfigureDeploymentStrategy() {
        String bridgeName = "example-bridge";

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
    void testCustomBridgeLabelsAreProperlySet() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String bridgeName = "bridge-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
    void createClassResources() {
        final String namespaceToWatch = Environment.isNamespaceRbacScope() ? CO_NAMESPACE : TestConstants.WATCH_ALL_NAMESPACES;
        suiteTestStorage = new TestStorage(ResourceManager.getTestContext());

        clusterOperator = clusterOperator.defaultInstallation()
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

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 1).build()
            )
        );
        // Deploy kafka
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(suiteTestStorage.getClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getClusterName(),
            KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
