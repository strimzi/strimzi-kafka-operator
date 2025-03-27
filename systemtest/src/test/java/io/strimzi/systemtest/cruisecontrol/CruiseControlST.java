/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfigurationBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIds;
import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIdsBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test suite validates the functionality and behavior of Cruise Control across multiple Kafka scenarios. " +
        "It ensures correct operation under various configurations and conditions."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default installation", expected = "Cluster Operator is deployed and running")
    },
    labels = {
        @Label(value = TestDocsLabels.CRUISE_CONTROL),
    }
)
public class CruiseControlST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test verifying the automatic creation and configuration of Cruise Control topics with resources."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Set up Kafka brokers and Cruise Control with necessary configurations", expected = "Resources are created with the desired configurations"),
            @Step(value = "Validate Cruise Control pod's memory resource limits and JVM options", expected = "Memory limits and JVM options are correctly set on the Cruise Control pod"),
            @Step(value = "Create a Kafka topic and an AdminClient", expected = "Kafka topic and AdminClient are successfully created"),
            @Step(value = "Verify Cruise Control topics are present", expected = "Cruise Control topics are found present in the configuration")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testAutoCreationOfCruiseControlTopicsWithResources() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // number of brokers to be created and also number of default replica count for each topic created
        final int defaultBrokerReplicaCount = 3;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), defaultBrokerReplicaCount)
            .editOrNewSpec()
                .editKafka()
                    .addToConfig(Map.of("default.replication.factor", defaultBrokerReplicaCount))
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
                .editCruiseControl()
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("300Mi"))
                        .addToRequests("memory", new Quantity("300Mi"))
                        .build())
                    .withNewJvmOptions()
                        .withXmx("200M")
                        .withXms("128M")
                        .withXx(Map.of("UseG1GC", "true"))
                    .endJvmOptions()
                .endCruiseControl()
            .endSpec()
            .build());

        String ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        Container container = (Container) KubeClusterResource.kubeClient().getPod(testStorage.getNamespaceName(), ccPodName).getSpec().getContainers().stream().filter(c -> c.getName().equals("cruise-control")).findFirst().get();
        assertThat(container.getResources().getLimits().get("memory"), is(new Quantity("300Mi")));
        assertThat(container.getResources().getRequests().get("memory"), is(new Quantity("300Mi")));
        VerificationUtils.assertJvmOptions(testStorage.getNamespaceName(), ccPodName, "cruise-control",
                "-Xmx200M", "-Xms128M", "-XX:+UseG1GC");

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );
        final AdminClient adminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(adminClient, defaultBrokerReplicaCount);
    }

    @IsolatedTest
    void testCruiseControlWithApiSecurityDisabled() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editOrNewSpec()
                    .editCruiseControl()
                        .addToConfig("webserver.security.enable", "false")
                        .addToConfig("webserver.ssl.enable", "false")
                    .endCruiseControl()
                .endSpec()
                .build());
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("Using Kafka cluster within a single namespace to test Cruise Control with rebalance resource and refresh annotation."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create Kafka cluster", expected = "Kafka cluster with ephemeral storage is created and available"),
            @Step(value = "Deploy Kafka Rebalance resource", expected = "Kafka Rebalance resource is deployed and in NotReady state"),
            @Step(value = "Enable Cruise Control with optimized configuration", expected = "Cruise Control is enabled and configured"),
            @Step(value = "Perform rolling update on broker pods", expected = "All broker pods have rolled successfully"),
            @Step(value = "Execute rebalance process", expected = "Rebalancing process executed successfully"),
            @Step(value = "Annotate Kafka Rebalance resource with 'refresh'", expected = "Kafka Rebalance resource is annotated with 'refresh' and reaches ProposalReady state"),
            @Step(value = "Execute rebalance process again", expected = "Rebalancing process re-executed successfully")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testCruiseControlWithRebalanceResourceAndRefreshAnnotation() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithoutWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.NotReady);

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        // CruiseControl spec is now enabled
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            // Get default CC spec with tune options and set it to existing Kafka
            Kafka kafkaUpdated = KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build();
            kafka.getSpec().setCruiseControl(kafkaUpdated.getSpec().getCruiseControl());
            kafka.getSpec().setKafka(kafkaUpdated.getSpec().getKafka());
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Annotating KafkaRebalance: {} with 'refresh' anno", testStorage.getClusterName());
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.refresh);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Trying rebalancing process again");
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test that ensures Cruise Control transitions from Rebalancing to ProposalReady state when the KafkaRebalance spec is updated."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create Kafka cluster with Cruise Control", expected = "Kafka cluster with Cruise Control is created and running"),
            @Step(value = "Create KafkaRebalance resource", expected = "KafkaRebalance resource is created and running"),
            @Step(value = "Wait until KafkaRebalance is in ProposalReady state", expected = "KafkaRebalance reaches ProposalReady state"),
            @Step(value = "Annotate KafkaRebalance with 'approve'", expected = "KafkaRebalance is annotated with approval"),
            @Step(value = "Update KafkaRebalance spec to configure replication throttle", expected = "KafkaRebalance resource's spec is updated"),
            @Step(value = "Wait until KafkaRebalance returns to ProposalReady state", expected = "KafkaRebalance re-enters ProposalReady state following the update")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testCruiseControlChangesFromRebalancingtoProposalReadyWhenSpecUpdated() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Annotating KafkaRebalance: {} with 'approve' anno", testStorage.getClusterName());
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);

        // updating the KafkaRebalance resource by configuring replication throttle
        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaRebalance -> kafkaRebalance.getSpec().setReplicationThrottle(100000));

        // the resource moved to `ProposalReady` state
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying that Cruise Control cannot be deployed with a Kafka cluster that has only one broker and ensuring that increasing the broker count resolves the configuration error."),
        steps = {
            @Step(value = "Set up the error message", expected = "Error message is set"),
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy single-node Kafka with Cruise Control", expected = "Kafka and Cruise Control deployment initiated"),
            @Step(value = "Verify that the Kafka status contains the error message related to single-node configuration", expected = "Error message confirmed in Kafka status"),
            @Step(value = "Increase the Kafka nodes to 3", expected = "Kafka node count increased to 3"),
            @Step(value = "Check that the Kafka status no longer contains the single-node error message", expected = "Error message resolved")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    @SuppressWarnings("deprecation") // Replicas in Kafka CR are deprecated, but some API methods are still called here
    void testCruiseControlWithSingleNodeKafka() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String errMessage =  "Kafka " + testStorage.getNamespaceName() + "/" + testStorage.getClusterName() + " has invalid configuration. " +
            "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers.";

        LOGGER.info("Deploying single node Kafka with CruiseControl");
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), errMessage, Duration.ofMinutes(6).toMillis());

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();

        assertThat(kafkaStatus.getConditions().stream().filter(c -> "InvalidResourceException".equals(c.getReason())).findFirst().orElse(null), is(notNullValue()));

        LOGGER.info("Increasing Kafka nodes to 3");

        int scaleTo = 3;

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(),
            knp -> knp.getSpec().setReplicas(scaleTo));

        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        kafkaStatus = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is(not(errMessage)));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verify that Kafka Cruise Control excludes specified topics and includes others."),
        steps = {
            @Step(value = "Set topic names", expected = "Topic names are set"),
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy Kafka with Cruise Control enabled", expected = "Kafka cluster with Cruise Control is deployed"),
            @Step(value = "Create topics to be excluded and included", expected = "Topics 'excluded-topic-1', 'excluded-topic-2', and 'included-topic' are created"),
            @Step(value = "Create KafkaRebalance resource excluding specific topics", expected = "KafkaRebalance resource is created with 'excluded-.*' topics pattern"),
            @Step(value = "Wait for KafkaRebalance to reach ProposalReady state", expected = "KafkaRebalance reaches the ProposalReady state"),
            @Step(value = "Check optimization result for excluded and included topics", expected = "Excluded topics are in the optimization result and included topic is not"),
            @Step(value = "Approve the KafkaRebalance proposal", expected = "KafkaRebalance proposal is approved"),
            @Step(value = "Wait for KafkaRebalance to reach Ready state", expected = "KafkaRebalance reaches the Ready state")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testCruiseControlTopicExclusion() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String excludedTopic1 = "excluded-topic-1";
        final String excludedTopic2 = "excluded-topic-2";
        final String includedTopic = "included-topic";

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), excludedTopic1, testStorage.getClusterName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), excludedTopic2, testStorage.getClusterName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), includedTopic, testStorage.getClusterName()).build());

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editOrNewSpec()
                .withExcludedTopics("excluded-.*")
            .endSpec()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic1));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic2));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), not(containsString(includedTopic)));

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.Ready);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test that verifies the configuration and application of custom Cruise Control replica movement strategies."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create Kafka and Cruise Control resources", expected = "Kafka and Cruise Control resources are created and deployed"),
            @Step(value = "Verify default Cruise Control replica movement strategy", expected = "Default replica movement strategy is verified in the configuration"),
            @Step(value = "Update Cruise Control configuration with non-default replica movement strategies", expected = "Cruise Control configuration is updated with new strategies"),
            @Step(value = "Ensure that Cruise Control pod is rolled due to configuration change", expected = "Cruise Control pod is rolled and new configuration is applied"),
            @Step(value = "Verify the updated Cruise Control configuration", expected = "Updated replica movement strategies are verified in Cruise Control configuration")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testCruiseControlReplicaMovementStrategy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String replicaMovementStrategies = "default.replica.movement.strategies";
        final String newReplicaMovementStrategies = "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy";

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        String ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        LOGGER.info("Check for default CruiseControl replicaMovementStrategy in Pod configuration file");
        Map<String, Object> actualStrategies = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName()).get().getSpec().getCruiseControl().getConfig();
        // Check that config contains only configurations for max.active.user.tasks, metric.sampling.interval.ms, cruise.control.metrics.reporter.metrics.reporting.interval.ms, metadata.max.age.ms
        assertThat(actualStrategies.size(), is(4));

        String ccConfFileContent = cmdKubeClient(testStorage.getNamespaceName()).execInPodContainer(ccPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "cat", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, not(containsString(replicaMovementStrategies)));

        Map<String, String> kafkaRebalanceSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        Map<String, Object> ccConfigMap = new HashMap<>();
        ccConfigMap.put(replicaMovementStrategies, newReplicaMovementStrategies);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            LOGGER.info("Set non-default CruiseControl replicaMovementStrategies to KafkaRebalance resource");
            kafka.getSpec().getCruiseControl().setConfig(ccConfigMap);
        });

        LOGGER.info("Verifying that CC Pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, kafkaRebalanceSnapshot);

        ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        ccConfFileContent = cmdKubeClient(testStorage.getNamespaceName()).execInPodContainer(ccPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "cat", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, containsString(newReplicaMovementStrategies));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test ensuring the intra-broker disk balancing with Cruise Control works as expected."),
        steps = {
            @Step(value = "Initialize JBOD storage configuration", expected = "JBOD storage with specific disk sizes are initialized"),
            @Step(value = "Create Kafka broker and controller pools using the initialized storage", expected = "Kafka broker and controller pools are created and available"),
            @Step(value = "Deploy Kafka with Cruise Control enabled", expected = "Kafka deployment with Cruise Control is successfully created"),
            @Step(value = "Create Kafka Rebalance resource with disk rebalancing configured", expected = "Kafka Rebalance resource is created and configured for disk balancing"),
            @Step(value = "Wait for the Kafka Rebalance to reach the ProposalReady state", expected = "Kafka Rebalance resource reaches the ProposalReady state"),
            @Step(value = "Check the status of the Kafka Rebalance for intra-broker disk balancing", expected = "The 'provisionStatus' in the optimization result is 'UNDECIDED'")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testCruiseControlIntraBrokerBalancing() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String diskSize = "6Gi";

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSize).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSize).build()
                ).build();

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withStorage(jbodStorage)
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editOrNewSpec()
                    .editKafka()
                        .withStorage(jbodStorage)
                    .endKafka()
                .endSpec()
                .build());
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withRebalanceDisk(true)
                .endSpec()
                .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        // The provision status should be "UNDECIDED" when doing an intra-broker disk balance because it is irrelevant to the provision status
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("provisionStatus").toString(), containsString("UNDECIDED"));
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Testing the behavior of Cruise Control during both scaling up and down of Kafka brokers using KafkaNodePools."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create initial Kafka cluster setup with Cruise Control and topic", expected = "Kafka cluster, topic, and scraper pod are created successfully"),
            @Step(value = "Scale Kafka up to a higher number of brokers", expected = "Kafka brokers are scaled up to the specified number of replicas"),
            @Step(value = "Create a KafkaRebalance resource with add_brokers mode", expected = "KafkaRebalance proposal is ready and processed for adding brokers"),
            @Step(value = "Check the topic's replicas on the new brokers", expected = "Topic has replicas on one of the newly added brokers"),
            @Step(value = "Create a KafkaRebalance resource with remove_brokers mode", expected = "KafkaRebalance proposal is ready and processed for removing brokers"),
            @Step(value = "Check the topic's replicas only on initial brokers", expected = "Topic replicas are only on the initial set of brokers"),
            @Step(value = "Scale Kafka down to the initial number of brokers", expected = "Kafka brokers are scaled down to the original number of replicas")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    @SuppressWarnings("deprecation") // Replicas in Kafka CR are deprecated, but some API methods are still called here
    void testCruiseControlDuringBrokerScaleUpAndDown() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        final int initialReplicas = 3;
        final int scaleTo = 5;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas)
                .editOrNewMetadata()
                    // controllers have Ids set in order to keep default ordering for brokers only (once we scale broker KNP)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-103]"))
                .endMetadata()
            .build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 10, 3).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Scaling Kafka up to {}", scaleTo);

        // Collect CC pod snapshot to wait for rolling
        Map<String, String> cruiseControlSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaleTo));

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaleTo);

        LOGGER.info("Wait for Cruise Control rolling update after scale-up");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, cruiseControlSnapshot);

        LOGGER.info("Creating KafkaRebalance with add_brokers mode");

        // when using add_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.ADD_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).delete();

        LOGGER.info("Checking that Topic: {} has replicas on one of the new brokers (or both)", testStorage.getTopicName());
        List<String> topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertTrue(topicReplicas.stream().anyMatch(line -> line.contains("3") || line.contains("4")));

        LOGGER.info("Creating KafkaRebalance with remove_brokers mode - it needs to be done before actual scaling down of Kafka Pods");

        // when using remove_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking that Topic: {} has replicas only on first 3 brokers", testStorage.getTopicName());
        topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertEquals(0, (int) topicReplicas.stream().filter(line -> line.contains("3") || line.contains("4")).count());

        LOGGER.info("Scaling Kafka down to {}", initialReplicas);

        // Collect CC pod snapshot to wait for rolling
        cruiseControlSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(initialReplicas));

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialReplicas);

        LOGGER.info("Wait for Cruise Control rolling update after scale-down");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, cruiseControlSnapshot);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test the Kafka Rebalance auto-approval mechanism."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy Kafka cluster with Cruise Control", expected = "Kafka cluster with Cruise Control is deployed"),
            @Step(value = "Create KafkaRebalance resource with auto-approval enabled", expected = "KafkaRebalance resource with auto-approval is created"),
            @Step(value = "Perform re-balancing process with auto-approval", expected = "Re-balancing process completes successfully with auto-approval")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testKafkaRebalanceAutoApprovalMechanism() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        // KafkaRebalance with auto-approval
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
            .endMetadata()
            .build()
        );

        KafkaRebalanceUtils.doRebalancingProcessWithAutoApproval(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test the Kafka Cruise Control auto-rebalance mechanism during scaling up and down of brokers."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created."),
            @Step(value = "Create KafkaRebalance templates for scale-up and scale-down operations.", expected = "KafkaRebalance templates are created and annotated as configuration templates."),
            @Step(value = "Deploy Kafka cluster with Cruise Control using the defined templates for auto-rebalance.", expected = "Kafka cluster with Cruise Control is deployed, configured with the specified auto-rebalance templates."),
            @Step(value = "Ensure the Kafka auto-rebalance status is Idle.", expected = "Kafka auto-rebalance status is confirmed to be Idle."),
            @Step(value = "Scale Kafka up to a higher number of brokers.", expected = "Kafka brokers are scaled up, and Cruise Control initiates rebalancing in ADD_BROKERS mode."),
            @Step(value = "Verify that Kafka auto-rebalance status transitions to RebalanceOnScaleUp and then back to Idle.", expected = "Auto-rebalance status moves to RebalanceOnScaleUp during scaling and returns to Idle after rebalancing completes."),
            @Step(value = "Check that topic replicas are moved to the new brokers.", expected = "Topic replicas are distributed onto the newly added brokers."),
            @Step(value = "Change number of replicas of Kafka cluster to initial replicas within KafkaNodePool (i.e., 3 brokers)", expected = "KafkaNodePool is set successfully to 3 replicas and auto-rebalance is triggered."),
            @Step(value = "After auto-rebalance is done, Kafka cluster is scale-down to the original number of brokers.", expected = "Kafka brokers are scaled down because there are no hosted partitions anymore that would prevent such an operation."),
            @Step(value = "Verify that Kafka auto-rebalance status transitions to RebalanceOnScaleDown and then back to Idle.", expected = "Auto-rebalance status moves to RebalanceOnScaleDown during scaling down and returns to Idle after rebalancing completes."),
            @Step(value = "Confirm that the cluster is stable after scaling operations.", expected = "Cluster returns to a stable state with initial number of brokers and Cruise Control completed the rebalancing.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    @SuppressWarnings("deprecation") // Replicas in Kafka CR are deprecated, but some API methods are still called here
    void testAutoKafkaRebalanceScaleUpScaleDown() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String scaleUpKafkaRebalanceTemplateName = testStorage.getClusterName() + "-scale-up";
        final String scaleDownKafkaRebalanceTemplateName = testStorage.getClusterName() + "-scale-down";
        final int initialReplicas = 3;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas)
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-103]"))
                .endMetadata()
                .build()
        );

        // create two KafkaRebalance with marking resource as configuration template
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(scaleUpKafkaRebalanceTemplateName)
                .endMetadata()
                .build(),
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(scaleDownKafkaRebalanceTemplateName)
                .endMetadata()
                .build()
        );

        // define CC with two auto-rebalance modes (add/remove) and corresponding templates
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .editCruiseControl()
                        .withAutoRebalance(
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withNewTemplate(scaleUpKafkaRebalanceTemplateName)
                                .build(),
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.REMOVE_BROKERS)
                                .withNewTemplate(scaleDownKafkaRebalanceTemplateName)
                                .build())
                    .endCruiseControl()
                .endSpec()
            .build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        // check that Kafka has `Idle` in auto-rebalance status
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        final int scaleTo = 5;
        Map<String, String> ccPod = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Scaling Kafka up to {}", scaleTo);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaleTo));

        final int kafkaClusterPodIndex = initialReplicas;

        KafkaUtils.waitUntilKafkaHasExpectedAutoRebalanceModeAndBrokers(testStorage.getNamespaceName(), testStorage.getClusterName(),
            KafkaAutoRebalanceMode.ADD_BROKERS,
            // brokers with [3, 4]
            Arrays.asList(kafkaClusterPodIndex, kafkaClusterPodIndex + 1));

        // check that KafkaRebalance <cluster-name>-auto-rebalancing-<mode> is created
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(), KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.ADD_BROKERS));

        // check that Kafka has `RebalanceOnScaleUp` in auto-rebalance status
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnScaleUp);

        // CC Rolled
        ccPod = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, ccPod);

        // we double-check that Rolling has been done.
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaleTo);

        // and then afterward we again move to Idle state after all is done
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        // check that KafkaRebalance <cluster-name>-auto-rebalancing-<mode> is deleted
        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.ADD_BROKERS));

        KafkaTopicUtils.waitForTopicReplicasOnBrokers(testStorage.getNamespaceName(), testStorage.getTopicName(),
            scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), Arrays.asList("3", "4"));

        LOGGER.info("Scaling Kafka down to {}", initialReplicas);

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(initialReplicas));

        KafkaUtils.waitUntilKafkaHasExpectedAutoRebalanceModeAndBrokers(testStorage.getNamespaceName(), testStorage.getClusterName(),
            KafkaAutoRebalanceMode.REMOVE_BROKERS,
            // brokers with [3, 4]
            Arrays.asList(kafkaClusterPodIndex, kafkaClusterPodIndex + 1));

        // check that KafkaRebalance <cluster-name>-auto-rebalancing-<mode> is created
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(), KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.REMOVE_BROKERS));

        // check that Kafka has `RebalanceOnScaleDown` in auto-rebalance status
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnScaleDown);

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialReplicas);

        // CC is rolled
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, ccPod);

        // and then afterward we again move to Idle state after all is done
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        // check that KafkaRebalance <cluster-name>-auto-rebalancing-<mode> is deleted
        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.REMOVE_BROKERS));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying the 'remove-disks' mode in Cruise Control, which allows moving data between JBOD disks on the same broker."),
        steps = {
            @Step(value = "Initialize JBOD storage configuration with multiple volumes (disks).", expected = "JBOD storage with disk IDs 0, 1, and 2 are initialized."),
            @Step(value = "Deploy Kafka with Cruise Control enabled.", expected = "Kafka with Cruise Control is successfully created."),
            @Step(value = "Create KafkaTopic resource and produce data to it.", expected = "KafkaTopic resource is created and data is produced to it."),
            @Step(value = "Retrieve initial data directory sizes and partition replicas for the disks being removed.", expected = "Initial data directory sizes and partition replicas are retrieved."),
            @Step(value = "Create a KafkaRebalance resource with 'remove-disks' mode, specifying the brokers and volume IDs.", expected = "KafkaRebalance resource is created with 'remove-disks' mode and moveReplicasOffVolumes settings."),
            @Step(value = "Wait for the KafkaRebalance to reach the ProposalReady state.", expected = "KafkaRebalance reaches the ProposalReady state."),
            @Step(value = "Approve the KafkaRebalance proposal.", expected = "KafkaRebalance is approved."),
            @Step(value = "Wait for the KafkaRebalance to reach Ready state.", expected = "KafkaRebalance reaches the Ready state."),
            @Step(value = "Verify that data has been moved off the specified disks by checking data directory sizes in the broker pods.", expected = "Data directories for the specified volumes are empty or have minimal data, confirming data has been moved off."),
            @Step(value = "Verify that partitions have been moved off the specified disks.", expected = "Partitions are no longer present on the specified disks, confirming successful removal."),
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testCruiseControlRemoveDisksMode() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String diskSize = "6Gi";

        // Initialize JBOD storage configuration with disk IDs 0, 1, and 2
        JbodStorage jbodStorage = new JbodStorageBuilder()
            .withVolumes(
                new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSize).build(),
                new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSize).build(),
                new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(2).withSize(diskSize).build()
            ).build();

        // Create Kafka broker and controller pools using the initialized storage
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withStorage(jbodStorage)
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        // Deploy Kafka with Cruise Control enabled
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editOrNewSpec()
                    .editKafka()
                        .withStorage(jbodStorage)
                    .endKafka()
                .endSpec()
                .build());

        // Create topics and produce some data to them
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 24, 3).build());

        // send some data to topic
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClientBuilder(testStorage).withMessageCount(1000).build();
        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        final List<BrokerAndVolumeIds> brokersWithRemovedVolumes = Arrays.asList(
            new BrokerAndVolumeIdsBuilder().withBrokerId(0).withVolumeIds(Arrays.asList(1, 2)).build(),
            new BrokerAndVolumeIdsBuilder().withBrokerId(2).withVolumeIds(Arrays.asList(1)).build()
        );

        final Map<String, Long> initialDataDirSizes = KafkaTopicUtils.getDataDirectorySizes(testStorage, brokersWithRemovedVolumes);
        final Map<String, Set<Integer>> initialPartitionDirs = KafkaTopicUtils.getPartitionDirectories(testStorage, brokersWithRemovedVolumes);

        LOGGER.info("Initial data directory sizes: {}", initialDataDirSizes);
        LOGGER.info("Initial partition directories: {}", initialPartitionDirs);

        // Create a KafkaRebalance resource with 'remove-disks' mode, specifying the brokers and volume IDs
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.REMOVE_DISKS)
                    .withMoveReplicasOffVolumes(brokersWithRemovedVolumes)
                .endSpec()
                .build()
        );

        // Wait for the KafkaRebalance to reach ProposalReady state
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        // Approve the KafkaRebalance proposal
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);

        // Wait for the KafkaRebalance to reach Ready state
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.Ready);

        // Verify that data has been moved off the specified disks by checking data directory sizes
        KafkaTopicUtils.verifyDataMovedOffSpecifiedDisks(initialDataDirSizes, KafkaTopicUtils.getDataDirectorySizes(testStorage, brokersWithRemovedVolumes));

        // Verify that partitions have been moved off the specified disks
        KafkaTopicUtils.verifyPartitionsMovedOffDisks(initialPartitionDirs, KafkaTopicUtils.getPartitionDirectories(testStorage, brokersWithRemovedVolumes));
    }

    @BeforeAll
    void setUp() {
        // Cruise Control currently does not work with Kafka 4.0 and this tests is therefore disabled when Kafka 4.0 or newer is used.
        // This should be re-enabled once Cruise Control support is fixed: https://github.com/strimzi/strimzi-kafka-operator/issues/11199
        Assumptions.assumeTrue(TestKafkaVersion.compareDottedVersions(Environment.ST_KAFKA_VERSION, "4.0.0") < 0);

        setupClusterOperator
            .withDefaultConfiguration()
            .install();
    }
}
