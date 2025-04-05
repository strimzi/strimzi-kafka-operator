/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
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
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(REGRESSION)
@Tag(BRIDGE)
@SuiteDoc(
    description = @Desc("Test suite for various Kafka Bridge operations."),
    beforeTestSteps = {
        @Step(value = "Initialize Test Storage and deploy Kafka and Kafka Bridge.", expected = "Kafka and Kafka Bridge are deployed with necessary configuration.")
    },
    labels = {
        @Label(TestDocsLabels.BRIDGE),
    }
)
class HttpBridgeST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);

    private TestStorage suiteTestStorage;

    @ParallelTest
    @TestDoc(
        description = @Desc("Test validating that sending a simple message through Kafka Bridge works correctly and checks labels."),
        steps = {
            @Step(value = "Initialize test storage.", expected = "Test storage is initialized with necessary context."),
            @Step(value = "Create a Kafka Bridge client job.", expected = "Kafka Bridge client job is configured and instantiated."),
            @Step(value = "Create Kafka topic.", expected = "Kafka topic is successfully created."),
            @Step(value = "Start Kafka Bridge producer.", expected = "Kafka Bridge producer successfully begins sending messages."),
            @Step(value = "Wait for producer success.", expected = "All messages are sent successfully."),
            @Step(value = "Start Kafka consumer.", expected = "Kafka consumer is instantiated and starts consuming messages."),
            @Step(value = "Wait for consumer success.", expected = "All messages are consumed successfully."),
            @Step(value = "Verify Kafka Bridge pod labels.", expected = "Labels for Kafka Bridge pods are correctly set and verified."),
            @Step(value = "Verify Kafka Bridge service labels.", expected = "Labels for Kafka Bridge service are correctly set and verified.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE),
        }
    )
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
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        // Checking labels for KafkaBridge
        VerificationUtils.verifyPodsLabels(testStorage.getNamespaceName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()), KafkaBridgeResource.getLabelSelector(suiteTestStorage.getClusterName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName())));
        VerificationUtils.verifyServiceLabels(testStorage.getNamespaceName(), KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()), KafkaBridgeResource.getLabelSelector(suiteTestStorage.getClusterName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName())));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying that a simple message can be received using Kafka Bridge."),
        steps = {
            @Step(value = "Initialize the test storage.", expected = "TestStorage instance is initialized."),
            @Step(value = "Create Kafka topic resource.", expected = "Kafka topic resource is created with specified configurations."),
            @Step(value = "Setup and deploy Kafka Bridge consumer client.", expected = "Kafka Bridge consumer client is set up and started receiving messages."),
            @Step(value = "Send messages using Kafka producer.", expected = "Messages are sent to Kafka successfully."),
            @Step(value = "Verify message reception.", expected = "All messages are received by Kafka Bridge consumer client.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE),
        }
    )
    void testReceiveSimpleMessage() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

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

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test that validates the creation, update, and verification of a Kafka Bridge with specific initial and updated configurations."),
        steps = {
            @Step(value = "Create a Kafka Bridge resource with initial configuration.", expected = "Kafka Bridge is created and deployed with the specified initial configuration."),
            @Step(value = "Remove an environment variable that is in use.", expected = "Environment variable TEST_ENV_1 is removed from the initial configuration."),
            @Step(value = "Verify initial probe values and environment variables.", expected = "The probe values and environment variables match the initial configuration."),
            @Step(value = "Update Kafka Bridge resource with new configuration.", expected = "Kafka Bridge is updated and redeployed with the new configuration."),
            @Step(value = "Verify updated probe values and environment variables.", expected = "The probe values and environment variables match the updated configuration."),
            @Step(value = "Verify Kafka Bridge configurations for producer and consumer.", expected = "Producer and consumer configurations match the updated settings.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testCustomAndUpdatedValues() {

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

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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

        LOGGER.info("Verifying values before update");
        VerificationUtils.verifyReadinessAndLivenessProbes(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        VerificationUtils.verifyContainerEnvVariables(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), envVarGeneral);

        LOGGER.info("Updating values in Bridge container");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, bridgeName, kb -> {
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

        DeploymentUtils.waitTillDepHasRolled(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), 1, bridgeSnapshot);

        LOGGER.info("Verifying values after update");
        VerificationUtils.verifyReadinessAndLivenessProbes(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        VerificationUtils.verifyContainerEnvVariables(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), KafkaBridgeResources.componentName(bridgeName), envVarUpdated);

        ConfigMap configMap = kubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).getConfigMap(KafkaBridgeResources.configMapName(bridgeName));
        String bridgeConfiguration = configMap.getData().get("application.properties");
        Map<String, Object> config = StUtils.loadProperties(bridgeConfiguration);
        Map<String, Object> producerConfigMap = config.entrySet().stream().filter(e -> e.getKey().startsWith("kafka.producer.")).collect(Collectors.toMap(e -> e.getKey().replace("kafka.producer.", ""), Map.Entry::getValue));
        Map<String, Object> consumerConfigMap = config.entrySet().stream().filter(e -> e.getKey().startsWith("kafka.consumer.")).collect(Collectors.toMap(e -> e.getKey().replace("kafka.consumer.", ""), Map.Entry::getValue));
        assertThat(producerConfigMap.entrySet().containsAll(producerConfig.entrySet()), is(true));
        assertThat(consumerConfigMap.entrySet().containsAll(consumerConfig.entrySet()), is(true));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying the presence and correctness of the discovery annotation in the Kafka Bridge service."),
        steps = {
            @Step(value = "Retrieve the Kafka Bridge service using kubeClient.", expected = "Kafka Bridge service instance is obtained."),
            @Step(value = "Extract the discovery annotation from the service metadata.", expected = "The discovery annotation is retrieved as a string."),
            @Step(value = "Convert the discovery annotation to a JsonArray.", expected = "JsonArray representation of the discovery annotation is created."),
            @Step(value = "Validate the content of the JsonArray against expected values.", expected = "The JsonArray matches the expected service discovery information.")
        },
        labels = {
            @Label("service_discovery_verification"),
            @Label("annotation_validation")
        }
    )
    void testDiscoveryAnnotation() {
        Service bridgeService = kubeClient().getService(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()));
        String bridgeServiceDiscoveryAnnotation = bridgeService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(bridgeServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(8080, "http", "none", false)));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test that scales a KafkaBridge instance to zero replicas and verifies that it is properly handled."),
        steps = {
            @Step(value = "Create a KafkaBridge resource and wait for it to be ready.", expected = "KafkaBridge resource is created and ready with 1 replica."),
            @Step(value = "Fetch the current number of KafkaBridge pods.", expected = "There should be exactly 1 KafkaBridge pod initially."),
            @Step(value = "Scale KafkaBridge to zero replicas.", expected = "Scaling action is acknowledged."),
            @Step(value = "Wait for KafkaBridge to scale down to zero replicas.", expected = "KafkaBridge scales down to zero replicas correctly."),
            @Step(value = "Check the number of KafkaBridge pods after scaling", expected = "No KafkaBridge pods should be running"),
            @Step(value = "Verify the status of KafkaBridge", expected = "KafkaBridge status should indicate it is ready with zero replicas")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testScaleBridgeToZero() {

        String bridgeName = "scaling-bridge-down";

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1).build());

        List<String> bridgePods = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodNames(Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        String deploymentName = KafkaBridgeResources.componentName(bridgeName);

        assertThat(bridgePods.size(), is(1));

        LOGGER.info("Scaling KafkaBridge to zero replicas");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, bridgeName, kafkaBridge -> kafkaBridge.getSpec().setReplicas(0));

        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName());
        PodUtils.waitForPodsReady(Environment.TEST_SUITE_NAMESPACE, kubeClient().getDeploymentSelectors(Environment.TEST_SUITE_NAMESPACE, deploymentName), 0, true);

        bridgePods = kubeClient().listPodNames(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), Labels.STRIMZI_CLUSTER_LABEL, bridgeName);
        KafkaBridgeStatus bridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus();

        assertThat(bridgePods.size(), is(0));
        assertThat(bridgeStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test checks the scaling of a KafkaBridge subresource and verifies the scaling operation."),
        steps = {
            @Step(value = "Initialize the KafkaBridge resource.", expected = "KafkaBridge resource is created in the specified namespace."),
            @Step(value = "Scale the KafkaBridge resource to the desired replicas.", expected = "KafkaBridge resource is scaled to the expected number of replicas."),
            @Step(value = "Verify the number of replicas.", expected = "The number of replicas is as expected and the observed generation is correct."),
            @Step(value = "Check pod naming conventions.", expected = "Pod names should match the naming convention and be consistent.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testScaleBridgeSubresource() {
        String bridgeName = "scaling-bridge-up";

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1).build());

        int scaleTo = 4;
        long bridgeObsGen = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get().getStatus().getObservedGeneration();
        String bridgeGenName = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodsByPrefixInName(bridgeName).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaBridge subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).scaleByName(KafkaBridge.RESOURCE_KIND, bridgeName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, KafkaBridgeResources.componentName(bridgeName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        StUtils.waitUntilSupplierIsSatisfied("KafkaBridge replica is 4 and observedGeneration is lower than 4",
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
    @TestDoc(
        description = @Desc("Test verifies KafkaBridge deployment strategy configuration and label updates with RECREATE and ROLLING_UPDATE strategies."),
        steps = {
            @Step(value = "Create KafkaBridge resource with deployment strategy RECREATE", expected = "KafkaBridge resource is created in RECREATE strategy"),
            @Step(value = "Add a label to KafkaBridge resource", expected = "KafkaBridge resource is recreated with new label"),
            @Step(value = "Check that observed generation is 1 and the label is present", expected = "Observed generation is 1 and label 'some=label' is present"),
            @Step(value = "Change deployment strategy to ROLLING_UPDATE", expected = "Deployment strategy is changed to ROLLING_UPDATE"),
            @Step(value = "Add another label to KafkaBridge resource", expected = "Pods are rolled with new label"),
            @Step(value = "Check that observed generation is 2 and the new label is present", expected = "Observed generation is 2 and label 'another=label' is present")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testConfigureDeploymentStrategy() {
        String bridgeName = "example-bridge";

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, bridgeName,
            kb -> kb.getMetadata().setLabels(Collections.singletonMap("some", "label")));
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1);

        KafkaBridge kafkaBridge = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaBridge.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaBridge.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaBridge.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing Deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, bridgeName,
            kb -> kb.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE));
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, bridgeName);

        LOGGER.info("Adding another label to KafkaBridge resource, Pods should be rolled");
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, bridgeName, kb -> kb.getMetadata().getLabels().put("another", "label"));
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");

        StUtils.waitUntilSupplierIsSatisfied("KafkaBridge observed generation and labels", () -> {
            final KafkaBridge kB = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get();

            return kB.getStatus().getObservedGeneration() == 2L &&
                kB.getMetadata().getLabels().toString().contains("another=label") &&
                kB.getSpec().getTemplate().getDeployment().getDeploymentStrategy().equals(DeploymentStrategy.ROLLING_UPDATE);
        });
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying if custom labels and annotations for Kafka Bridge services are properly set and validated."),
        steps = {
            @Step(value = "Initialize TestStorage", expected = "TestStorage instance is created with the current test context"),
            @Step(value = "Create Kafka Bridge resource with custom labels and annotations", expected = "Kafka Bridge resource is successfully created and available"),
            @Step(value = "Retrieve Kafka Bridge service with custom labels", expected = "Kafka Bridge service is retrieved with specified custom labels"),
            @Step(value = "Filter and validate custom labels and annotations", expected = "Custom labels and annotations match the expected values")
        },
        labels = {
            @Label("label"),
            @Label("annotation")
        }
    )
    void testCustomBridgeLabelsAreProperlySet() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String bridgeName = "bridge-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1).build());

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
        suiteTestStorage = new TestStorage(ResourceManager.getTestContext());

        SetupClusterOperator
            .get()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withExtraEnvVars(
                    new EnvVarBuilder()
                        .withName("STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_LABELS")
                        .withValue("app=bar")
                        .build(),
                    new EnvVarBuilder()
                        .withName("STRIMZI_CUSTOM_KAFKA_BRIDGE_SERVICE_ANNOTATIONS")
                        .withValue("bar=app")
                        .build()
                )
                .build()
            )
            .install();

        LOGGER.info("Deploying Kafka and KafkaBridge before tests");

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 1).build()
        );
        // Deploy kafka
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1).build());

        // Deploy http bridge
        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(),
            KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());
    }
}
