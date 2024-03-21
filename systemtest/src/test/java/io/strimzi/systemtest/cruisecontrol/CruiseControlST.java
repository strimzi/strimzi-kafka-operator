/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.topic.KafkaTopicSpec;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.MixedRoleNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.UTONotSupported;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.SANITY;
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
public class CruiseControlST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);
    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2

    @IsolatedTest
    @UTONotSupported("https://github.com/strimzi/strimzi-kafka-operator/issues/8864")
    void testAutoCreationOfCruiseControlTopicsWithResources() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // number of brokers to be created and also number of default replica count for each topic created
        final int defaultBrokerReplicaCount = 3;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), defaultBrokerReplicaCount, defaultBrokerReplicaCount)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
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

        String ccPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        Container container = (Container) KubeClusterResource.kubeClient(Environment.TEST_SUITE_NAMESPACE).getPod(Environment.TEST_SUITE_NAMESPACE, ccPodName).getSpec().getContainers().stream().filter(c -> c.getName().equals("cruise-control")).findFirst().get();
        assertThat(container.getResources().getLimits().get("memory"), is(new Quantity("300Mi")));
        assertThat(container.getResources().getRequests().get("memory"), is(new Quantity("300Mi")));
        VerificationUtils.assertJvmOptions(Environment.TEST_SUITE_NAMESPACE, ccPodName, "cruise-control",
                "-Xmx200M", "-Xms128M", "-XX:+UseG1GC");

        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, CRUISE_CONTROL_METRICS_TOPIC);
        KafkaTopicSpec metricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE)
            .withName(CRUISE_CONTROL_METRICS_TOPIC).get().getSpec();

        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        KafkaTopicSpec modelTrainingTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE)
            .withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get().getSpec();

        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        KafkaTopicSpec partitionMetricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE)
            .withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get().getSpec();

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_METRICS_TOPIC);
        assertThat(metricsTopic.getPartitions(), is(1));
        assertThat(metricsTopic.getReplicas(), is(defaultBrokerReplicaCount));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        assertThat(modelTrainingTopic.getPartitions(), is(32));
        assertThat(modelTrainingTopic.getReplicas(), is(defaultBrokerReplicaCount));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        assertThat(partitionMetricsTopic.getPartitions(), is(32));
        assertThat(partitionMetricsTopic.getReplicas(), is(defaultBrokerReplicaCount));
    }

    @IsolatedTest
    void testCruiseControlWithApiSecurityDisabled() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
                .editMetadata()
                  .withNamespace(Environment.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .editOrNewSpec()
                    .editCruiseControl()
                        .addToConfig("webserver.security.enable", "false")
                        .addToConfig("webserver.ssl.enable", "false")
                    .endCruiseControl()
                .endSpec()
                .build());
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    void testCruiseControlWithRebalanceResourceAndRefreshAnnotation() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
                .editMetadata()
                    .withNamespace(Environment.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .build());
        resourceManager.createResourceWithoutWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editMetadata()
                    .withNamespace(Environment.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), KafkaRebalanceState.NotReady);

        Map<String, String> brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector());

        // CruiseControl spec is now enabled
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().setCruiseControl(new CruiseControlSpec()), Environment.TEST_SUITE_NAMESPACE);

        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), 3, brokerPods);

        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName()), Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());

        LOGGER.info("Annotating KafkaRebalance: {} with 'refresh' anno", testStorage.getClusterName());
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName()), Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), KafkaRebalanceAnnotation.refresh);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Trying rebalancing process again");
        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName()), Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @IsolatedTest
    void testCruiseControlChangesFromRebalancingtoProposalReadyWhenSpecUpdated() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(clusterOperator.getDeploymentNamespace(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(clusterOperator.getDeploymentNamespace(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 1).build());

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editMetadata()
                    .withNamespace(clusterOperator.getDeploymentNamespace())
                .endMetadata()
                .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterOperator.getDeploymentNamespace(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Annotating KafkaRebalance: {} with 'approve' anno", testStorage.getClusterName());
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, clusterOperator.getDeploymentNamespace(), testStorage.getClusterName()), clusterOperator.getDeploymentNamespace(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);

        // updating the KafkaRebalance resource by configuring replication throttle
        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(testStorage.getClusterName(), kafkaRebalance -> kafkaRebalance.getSpec().setReplicationThrottle(100000), clusterOperator.getDeploymentNamespace());

        // the resource moved to `ProposalReady` state
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterOperator.getDeploymentNamespace(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
    }

    @ParallelNamespaceTest
    void testCruiseControlWithSingleNodeKafka() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String errMessage =  "Kafka " + testStorage.getNamespaceName() + "/" + testStorage.getClusterName() + " has invalid configuration. " +
            "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers.";

        LOGGER.info("Deploying single node Kafka with CruiseControl");
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 1, 1).build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getClusterName(), testStorage.getNamespaceName(), errMessage, Duration.ofMinutes(6).toMillis());

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();

        assertThat(kafkaStatus.getConditions().stream().filter(c -> "InvalidResourceException".equals(c.getReason())).findFirst().orElse(null), is(notNullValue()));

        LOGGER.info("Increasing Kafka nodes to 3");

        int scaleTo = 3;

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(scaleTo), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(3), testStorage.getNamespaceName());
        }
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        kafkaStatus = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is(not(errMessage)));
    }

    @ParallelNamespaceTest
    void testCruiseControlTopicExclusion() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String excludedTopic1 = "excluded-topic-1";
        final String excludedTopic2 = "excluded-topic-2";
        final String includedTopic = "included-topic";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 1).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), excludedTopic1, testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), excludedTopic2, testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), includedTopic, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
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

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.Ready);
    }

    @ParallelNamespaceTest
    void testCruiseControlReplicaMovementStrategy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String replicaMovementStrategies = "default.replica.movement.strategies";
        final String newReplicaMovementStrategies = "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());

        String ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        LOGGER.info("Check for default CruiseControl replicaMovementStrategy in Pod configuration file");
        Map<String, Object> actualStrategies = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName()).get().getSpec().getCruiseControl().getConfig();
        // Check that config contains only configurations for max.active.user.tasks
        assertThat(actualStrategies.size(), is(1));

        String ccConfFileContent = cmdKubeClient(testStorage.getNamespaceName()).execInPodContainer(ccPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "cat", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, not(containsString(replicaMovementStrategies)));

        Map<String, String> kafkaRebalanceSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        Map<String, Object> ccConfigMap = new HashMap<>();
        ccConfigMap.put(replicaMovementStrategies, newReplicaMovementStrategies);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            LOGGER.info("Set non-default CruiseControl replicaMovementStrategies to KafkaRebalance resource");
            kafka.getSpec().getCruiseControl().setConfig(ccConfigMap);
        }, testStorage.getNamespaceName());

        LOGGER.info("Verifying that CC Pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, kafkaRebalanceSnapshot);

        ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        ccConfFileContent = cmdKubeClient(testStorage.getNamespaceName()).execInPodContainer(ccPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "cat", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, containsString(newReplicaMovementStrategies));
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testCruiseControlIntraBrokerBalancing() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String diskSize = "6Gi";

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSize).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSize).build()
                ).build();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                    .editSpec()
                        .withStorage(jbodStorage)
                    .endSpec()
                    .build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
                .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                    .editOrNewSpec()
                        .editKafka()
                            .withStorage(jbodStorage)
                        .endKafka()
                    .endSpec()
                .build());
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
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

    @ParallelNamespaceTest
    void testCruiseControlIntraBrokerBalancingWithoutSpecifyingJBODStorage() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());
        resourceManager.createResourceWithoutWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editOrNewSpec()
                .withRebalanceDisk(true)
            .endSpec()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.NotReady);

        // The intra-broker rebalance will fail for Kafka clusters with a non-JBOD configuration.
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getConditions().get(0).getReason(), is("InvalidResourceException"));
    }

    @IsolatedTest
    @MixedRoleNotSupported("Scaling a Kafka Node Pool with mixed roles is not supported yet")
    void testCruiseControlDuringBrokerScaleUpAndDown() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        final int initialReplicas = 3;
        final int scaleTo = 5;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas)
                    .editOrNewMetadata()
                        // controllers have Ids set in order to keep default ordering for brokers only (once we scale broker KNP)
                        .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-103]"))
                    .endMetadata()
                .build()
            )

        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), initialReplicas, initialReplicas).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 10, 3, testStorage.getNamespaceName()).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Scaling Kafka up to {}", scaleTo);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(scaleTo), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(scaleTo), testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaleTo);

        LOGGER.info("Creating KafkaRebalance with add_brokers mode");

        // when using add_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.ADD_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).delete();

        LOGGER.info("Checking that Topic: {} has replicas on one of the new brokers (or both)", testStorage.getTopicName());
        List<String> topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertTrue(topicReplicas.stream().anyMatch(line -> line.contains("3") || line.contains("4")));

        LOGGER.info("Creating KafkaRebalance with remove_brokers mode - it needs to be done before actual scaling down of Kafka Pods");

        // when using remove_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking that Topic: {} has replicas only on first 3 brokers", testStorage.getTopicName());
        topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertEquals(0, (int) topicReplicas.stream().filter(line -> line.contains("3") || line.contains("4")).count());

        LOGGER.info("Scaling Kafka down to {}", initialReplicas);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(initialReplicas), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(initialReplicas), testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialReplicas);
    }

    @ParallelNamespaceTest
    void testKafkaRebalanceAutoApprovalMechanism() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());

        // KafkaRebalance with auto-approval
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
            .endMetadata()
            .build()
        );

        KafkaRebalanceUtils.doRebalancingProcessWithAutoApproval(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND,
                testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @BeforeAll
    void setUp() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
