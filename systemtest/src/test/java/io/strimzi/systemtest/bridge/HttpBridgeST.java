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
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.CrdClients;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(REGRESSION)
@Tag(BRIDGE)
@SuiteDoc(
    description = @Desc("Test suite for various HTTP Bridge operations."),
    beforeTestSteps = {
        @Step(value = "Initialize Test Storage and deploy Kafka and HTTP Bridge.", expected = "Kafka and HTTP Bridge are deployed with necessary configuration.")
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
        description = @Desc("Test validating that sending a simple message through HTTP Bridge works correctly and checks labels."),
        steps = {
            @Step(value = "Initialize test storage.", expected = "Test storage is initialized with necessary context."),
            @Step(value = "Create a HTTP Bridge client job.", expected = "HTTP Bridge client job is configured and instantiated."),
            @Step(value = "Create Kafka topic.", expected = "Kafka topic is successfully created."),
            @Step(value = "Start HTTP Bridge producer.", expected = "HTTP Bridge producer successfully begins sending messages."),
            @Step(value = "Wait for producer success.", expected = "All messages are sent successfully."),
            @Step(value = "Start Kafka consumer.", expected = "Kafka consumer is instantiated and starts consuming messages."),
            @Step(value = "Wait for consumer success.", expected = "All messages are consumed successfully."),
            @Step(value = "Verify HTTP Bridge pod labels.", expected = "Labels for HTTP Bridge pods are correctly set and verified."),
            @Step(value = "Verify HTTP Bridge service labels.", expected = "Labels for HTTP Bridge service are correctly set and verified.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE),
        }
    )
    void testSendSimpleMessage() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

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
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        // Checking labels for HTTP Bridge
        VerificationUtils.verifyPodsLabels(testStorage.getNamespaceName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()), LabelSelectors.bridgeLabelSelector(suiteTestStorage.getClusterName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName())));
        VerificationUtils.verifyServiceLabels(testStorage.getNamespaceName(), KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName()), LabelSelectors.bridgeLabelSelector(suiteTestStorage.getClusterName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName())));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying that a simple message can be received using HTTP Bridge."),
        steps = {
            @Step(value = "Initialize the test storage.", expected = "TestStorage instance is initialized."),
            @Step(value = "Create Kafka topic resource.", expected = "Kafka topic resource is created with specified configurations."),
            @Step(value = "Setup and deploy HTTP Bridge consumer client.", expected = "HTTP Bridge consumer client is set up and started receiving messages."),
            @Step(value = "Send messages using Kafka producer.", expected = "Messages are sent to Kafka successfully."),
            @Step(value = "Verify message reception.", expected = "All messages are received by HTTP Bridge consumer client.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE),
        }
    )
    void testReceiveSimpleMessage() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

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
        KubeResourceManager.get().createResourceWithWait(kafkaBridgeClientJob.consumerStrimziBridge());

        // Send messages to Kafka
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi());

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test that validates the creation, update, and verification of a HTTP Bridge with specific initial and updated configurations."),
        steps = {
            @Step(value = "Create a KafkaBridge resource with initial configuration.", expected = "HTTP Bridge is created and deployed with the specified initial configuration."),
            @Step(value = "Remove an environment variable that is in use.", expected = "Environment variable TEST_ENV_1 is removed from the initial configuration."),
            @Step(value = "Verify initial probe values and environment variables.", expected = "The probe values and environment variables match the initial configuration."),
            @Step(value = "Update KafkaBridge resource with new configuration.", expected = "HTTP Bridge is updated and redeployed with the new configuration."),
            @Step(value = "Verify updated probe values and environment variables.", expected = "The probe values and environment variables match the updated configuration."),
            @Step(value = "Verify HTTP Bridge configurations for producer and consumer.", expected = "Producer and consumer configurations match the updated settings.")
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

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
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
        KafkaBridgeUtils.replace(Environment.TEST_SUITE_NAMESPACE, bridgeName, kb -> {
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

        ConfigMap configMap = KubeResourceManager.get().kubeClient().getClient().configMaps().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(KafkaBridgeResources.configMapName(bridgeName)).get();
        String bridgeConfiguration = configMap.getData().get("application.properties");
        Map<String, Object> config = StUtils.loadProperties(bridgeConfiguration);
        Map<String, Object> producerConfigMap = config.entrySet().stream().filter(e -> e.getKey().startsWith("kafka.producer.")).collect(Collectors.toMap(e -> e.getKey().replace("kafka.producer.", ""), Map.Entry::getValue));
        Map<String, Object> consumerConfigMap = config.entrySet().stream().filter(e -> e.getKey().startsWith("kafka.consumer.")).collect(Collectors.toMap(e -> e.getKey().replace("kafka.consumer.", ""), Map.Entry::getValue));
        assertThat(producerConfigMap.entrySet().containsAll(producerConfig.entrySet()), is(true));
        assertThat(consumerConfigMap.entrySet().containsAll(consumerConfig.entrySet()), is(true));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying the presence and correctness of the discovery annotation in the HTTP Bridge service."),
        steps = {
            @Step(value = "Retrieve the HTTP Bridge service using kubeClient.", expected = "HTTP Bridge service instance is obtained."),
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
        Service bridgeService = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(KafkaBridgeResources.serviceName(suiteTestStorage.getClusterName())).get();
        String bridgeServiceDiscoveryAnnotation = bridgeService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(bridgeServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(8080, "http", "none", false)));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test that scales a HTTP Bridge instance to zero replicas and verifies that it is properly handled."),
        steps = {
            @Step(value = "Create a KafkaBridge resource and wait for it to be ready.", expected = "KafkaBridge resource is created and ready with 1 replica."),
            @Step(value = "Fetch the current number of HTTP Bridge pods.", expected = "There should be exactly 1 HTTP Bridge pod initially."),
            @Step(value = "Scale HTTP Bridge to zero replicas.", expected = "Scaling action is acknowledged."),
            @Step(value = "Wait for HTTP Bridge to scale down to zero replicas.", expected = "HTTP Bridge scales down to zero replicas correctly."),
            @Step(value = "Check the number of HTTP Bridge pods after scaling", expected = "No HTTP Bridge pods should be running"),
            @Step(value = "Verify the status of HTTP Bridge", expected = "HTTP Bridge status should indicate it is ready with zero replicas")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testScaleBridgeToZero() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1).build());

        List<String> bridgePods = PodUtils.listPodNames(testStorage.getNamespaceName(), testStorage.getBridgeSelector());
        String deploymentName = KafkaBridgeResources.componentName(testStorage.getClusterName());

        assertThat(bridgePods.size(), is(1));

        LOGGER.info("Scaling HTTP Bridge to zero replicas");
        KafkaBridgeUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaBridge -> kafkaBridge.getSpec().setReplicas(0));

        KafkaBridgeUtils.waitForKafkaBridgeReady(testStorage.getNamespaceName(), suiteTestStorage.getClusterName());
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(testStorage.getNamespaceName()).withName(deploymentName).get().getSpec().getSelector(), 0, true);

        bridgePods = PodUtils.listPodNames(testStorage.getNamespaceName(), testStorage.getBridgeSelector());
        KafkaBridgeStatus bridgeStatus = CrdClients.kafkaBridgeClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();

        assertThat(bridgePods.size(), is(0));
        assertThat(bridgeStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test checks the scaling of a HTTP Bridge subresource and verifies the scaling operation."),
        steps = {
            @Step(value = "Initialize the HTTP Bridge resource.", expected = "KafkaBridge resource is created in the specified namespace."),
            @Step(value = "Scale the HTTP Bridge resource to the desired replicas.", expected = "HTTP Bridge resource is scaled to the expected number of replicas."),
            @Step(value = "Verify the number of replicas.", expected = "The number of replicas is as expected and the observed generation is correct."),
            @Step(value = "Check pod naming conventions.", expected = "Pod names should match the naming convention and be consistent.")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testScaleBridgeSubresource() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1).build());

        int scaleTo = 4;
        long bridgeObsGen = CrdClients.kafkaBridgeClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        String bridgeGenName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getClusterName()).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling HTTP Bridge subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).scaleByName(KafkaBridge.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), KafkaBridgeResources.componentName(testStorage.getClusterName()), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        StUtils.waitUntilSupplierIsSatisfied("HTTP Bridge replica is 4 and observedGeneration is lower than 4",
            () -> {
                List<String> bridgePods = PodUtils.listPodNames(testStorage.getNamespaceName(), testStorage.getBridgeSelector());

                return bridgePods.size() == 4 &&
                    CrdClients.kafkaBridgeClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getReplicas() == 4 &&
                    CrdClients.kafkaBridgeClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getReplicas() == 4 &&
                    /*
                    observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
                    the observed generation is increased
                    */
                    bridgeObsGen < CrdClients.kafkaBridgeClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
            });

        for (final String pod : PodUtils.listPodNames(testStorage.getNamespaceName(), testStorage.getBridgeSelector())) {
            assertThat(pod.contains(bridgeGenName), is(true));
        }
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifies HTTP Bridge deployment strategy configuration and label updates with RECREATE and ROLLING_UPDATE strategies."),
        steps = {
            @Step(value = "Create HTTP Bridge resource with deployment strategy RECREATE", expected = "KafkaBridge resource is created in RECREATE strategy"),
            @Step(value = "Add a label to HTTP Bridge resource", expected = "HTTP Bridge resource is recreated with new label"),
            @Step(value = "Check that observed generation is 1 and the label is present", expected = "Observed generation is 1 and label 'some=label' is present"),
            @Step(value = "Change deployment strategy to ROLLING_UPDATE", expected = "Deployment strategy is changed to ROLLING_UPDATE"),
            @Step(value = "Add another label to HTTP Bridge resource", expected = "Pods are rolled with new label"),
            @Step(value = "Check that observed generation is 2 and the new label is present", expected = "Observed generation is 2 and label 'another=label' is present")
        },
        labels = {
            @Label(TestDocsLabels.BRIDGE)
        }
    )
    void testConfigureDeploymentStrategy() {
        String bridgeName = "example-bridge";

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String bridgeDepName = KafkaBridgeResources.componentName(bridgeName);

        LOGGER.info("Adding label to HTTP Bridge resource, the CR should be recreated");
        KafkaBridgeUtils.replace(Environment.TEST_SUITE_NAMESPACE, bridgeName,
            kb -> kb.getMetadata().setLabels(Collections.singletonMap("some", "label")));
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1);

        KafkaBridge kafkaBridge = CrdClients.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaBridge.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaBridge.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaBridge.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing Deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaBridgeUtils.replace(Environment.TEST_SUITE_NAMESPACE, bridgeName,
            kb -> kb.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE));
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, bridgeName);

        LOGGER.info("Adding another label to HTTP Bridge resource, Pods should be rolled");
        KafkaBridgeUtils.replace(Environment.TEST_SUITE_NAMESPACE, bridgeName, kb -> kb.getMetadata().getLabels().put("another", "label"));
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");

        StUtils.waitUntilSupplierIsSatisfied("HTTP Bridge observed generation and labels", () -> {
            final KafkaBridge kB = CrdClients.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(bridgeName).get();

            return kB.getStatus().getObservedGeneration() == 2L &&
                kB.getMetadata().getLabels().toString().contains("another=label") &&
                kB.getSpec().getTemplate().getDeployment().getDeploymentStrategy().equals(DeploymentStrategy.ROLLING_UPDATE);
        });
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying if custom labels and annotations for HTTP Bridge services are properly set and validated."),
        steps = {
            @Step(value = "Initialize TestStorage", expected = "TestStorage instance is created with the current test context"),
            @Step(value = "Create HTTP Bridge resource with custom labels and annotations", expected = "HTTP Bridge resource is successfully created and available"),
            @Step(value = "Retrieve HTTP Bridge service with custom labels", expected = "HTTP Bridge service is retrieved with specified custom labels"),
            @Step(value = "Filter and validate custom labels and annotations", expected = "Custom labels and annotations match the expected values")
        },
        labels = {
            @Label("label"),
            @Label("annotation")
        }
    )
    void testCustomBridgeLabelsAreProperlySet() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String bridgeName = "bridge-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, bridgeName, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1).build());

        // get service with custom labels
        final Service kafkaBridgeService = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(KafkaBridgeResources.serviceName(bridgeName)).get();

        // filter only app-bar service
        final Map<String, String> filteredActualKafkaBridgeCustomLabels =
            kafkaBridgeService.getMetadata().getLabels().entrySet().stream()
                .filter(item -> item.getKey().equals("app") && item.getValue().equals("bar"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final Map<String, String> filteredActualKafkaBridgeCustomAnnotations =
            kafkaBridgeService.getMetadata().getAnnotations().entrySet().stream()
                .filter(item -> item.getKey().equals("bar") && item.getValue().equals("app"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // verify phase: that inside KafkaBridge we can find 'exceptedKafkaBridgeCustomLabels' and 'exceptedKafkaBridgeCustomAnnotations' previously defined
        assertThat(filteredActualKafkaBridgeCustomLabels.size(), is(Collections.singletonMap("app", "bar").size()));
        assertThat(filteredActualKafkaBridgeCustomAnnotations.size(), is(Collections.singletonMap("bar", "app").size()));
        assertThat(filteredActualKafkaBridgeCustomLabels, is(Collections.singletonMap("app", "bar")));
        assertThat(filteredActualKafkaBridgeCustomAnnotations, is(Collections.singletonMap("bar", "app")));
    }

    @BeforeAll
    void createClassResources() {
        suiteTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
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

        LOGGER.info("Deploying Kafka and HTTP Bridge before tests");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 1).build()
        );
        // Deploy kafka
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1).build());

        // Deploy http bridge
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(),
            KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), 1)
            .editSpec()
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());
    }
}
