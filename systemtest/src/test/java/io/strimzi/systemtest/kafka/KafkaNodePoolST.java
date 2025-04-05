/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;


import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.strimzi.operator.common.Util.hashStub;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("This test suite verifies various functionalities of KafkaNodePools in a Kafka cluster."),
    beforeTestSteps = {
        @Step(value = "Ensure the environment is not using OLM or Helm and KafkaNodePools are enabled.", expected = "Environment is validated."),
        @Step(value = "Install the default Cluster Operator.", expected = "Cluster operator is installed.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class KafkaNodePoolST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaNodePoolST.class);

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies the management of broker IDs in KafkaNodePools using annotations."),
        steps = {
            @Step(value = "Deploy a Kafka instance with annotations to manage KafkaNodePools and one initial KafkaNodePool to hold topics and act as controller.", expected = "Kafka instance is deployed according to Kafka and KafkaNodePool CustomResource, with IDs 90, 91."),
            @Step(value = "Deploy additional 2 KafkaNodePools (A,B) with 1 and 2 replicas, and preset 'next-node-ids' annotations holding resp. values ([4],[6]).", expected = "KafkaNodePools are deployed, KafkaNodePool A contains ID 4, KafkaNodePool B contains IDs 6, 0."),
            @Step(value = "Annotate KafkaNodePool A 'next-node-ids' and KafkaNodePool B 'remove-node-ids' respectively ([20-21],[6,55]) afterward scale to 4 and 1 replicas resp.", expected = "KafkaNodePools are scaled, KafkaNodePool A contains IDs 4, 20, 21, 1. KafkaNodePool B contains ID 0."),
            @Step(value = "Annotate KafkaNodePool A 'remove-node-ids' and KafkaNodePool B 'next-node-ids' respectively ([20],[1]) afterward scale to 2 and 6 replicas resp.", expected = "KafkaNodePools are scaled, KafkaNodePool A contains IDs 1, 4. KafkaNodePool B contains IDs 2, 3, 5.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testKafkaNodePoolBrokerIdsManagementUsingAnnotations() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String nodePoolNameA = testStorage.getBrokerPoolName() + "-a";
        final String nodePoolNameB = testStorage.getBrokerPoolName() + "-b";
        final String nodePoolNameInitial = testStorage.getBrokerPoolName() + "-initial";

        // Deploy Initial NodePool (which will hold initial topics and will never be scaled down) with IDs far from those that will be used in the test
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3)
                .editOrNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-103]"))
                .endMetadata()
                .build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), nodePoolNameInitial, testStorage.getClusterName(), 2)
                .editOrNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[91-93]"))
                .endMetadata()
                .build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );

        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameInitial), 2);

        LOGGER.info("Testing deployment of KafkaNodePools with pre-configured annotation: {} is creating Brokers with correct IDs", Annotations.ANNO_STRIMZI_IO_NODE_POOLS);

        // Deploy NodePool A with only 1 replica and next ID 4, and NodePool B with 2 replica and next ID 6
        resourceManager.createResourceWithWait(KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), nodePoolNameA, testStorage.getClusterName(), 1)
                .editOrNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[4]"))
                .endMetadata()
                .build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), nodePoolNameB, testStorage.getClusterName(), 2)
                .editOrNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[6]"))
                .endMetadata()
                .build()
        );

        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 1);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 2);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [4]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).get(0).equals(4));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0, 6]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).equals(Arrays.asList(0, 6)));

        LOGGER.info("Testing annotation with upscaling NodePool A (more replicas than specified IDs) " +
                    "and downscaling NodePool B (more IDs than needed to be scaled down. This redundant ID is not present)");
        // Annotate NodePool A for scale up with fewer IDs than needed -> this should cause addition of non-used ID starting from [0] in ASC order, which is in this case [1]
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameA, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS,  "[20-21]"));
        // Annotate NodePool B for scale down with more IDs than needed - > this should not matter as ID [55] is not present so only ID [6] is removed
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameB, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[6, 55]"));

        // Scale NodePool A up + NodePool B down
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, 4);
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, 1);

        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 4);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 1);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [1, 4, 20, 21]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).equals(Arrays.asList(1, 4, 20, 21)));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).get(0).equals(0));

        // 3. Case (A-missing ID for downscale, B-already used ID for upscale)
        LOGGER.info("Testing annotation with downscaling NodePool A (fewer IDs than needed) and NodePool B (already used ID)");
        // Annotate NodePool A for scale down with fewer IDs than needed, this should cause removal of IDs in DESC order after the annotated ID is deleted
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameA, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS,  "[20]"));
        // Annotate NodePool B for scale up with ID [1] already in use
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameB, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1]"));

        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, 2);
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, 4);

        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 2);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 4);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [1, 4]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).equals(Arrays.asList(1, 4)));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0, 2, 3, 5]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).equals(Arrays.asList(0, 2, 3, 5)));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies changing of roles in KafkaNodePools."),
        steps = {
            @Step(value = "Deploy a Kafka instance with annotations to manage KafkaNodePools and 2 initial KafkaNodePools, both with mixed role, first one stable, second one which will be modified.", expected = "Kafka instance with initial KafkaNodePools is deployed."),
            @Step(value = "Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present.", expected = "KafkaTopic created."),
            @Step(value = "Deploy clients and transmit messages and remove KafkaTopic.", expected = "Transition of messages is finished successfully."),
            @Step(value = "Remove KafkaTopic.", expected = "KafkaTopic is cleaned as expected."),
            @Step(value = "Annotate one of KafkaNodePools to perform manual rolling update.", expected = "Rolling update started."),
            @Step(value = "Change role of KafkaNodePool from mixed to controller only role.", expected = "Role Change is prevented due to existing KafkaTopic replicas and ongoing rolling update."),
            @Step(value = "Original rolling update finishes successfully.", expected = "Rolling update is completed."),
            @Step(value = "Delete previously created KafkaTopic.", expected = "KafkaTopic is deleted and KafkaNodePool role change is initiated."),
            @Step(value = "Change role of KafkaNodePool from controller only to mixed role.", expected = "KafkaNodePool changes role to mixed role."),
            @Step(value = "Produce and consume messages on newly created KafkaTopic with replica count requiring also new brokers to be present.", expected = "Messages are produced and consumed successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testNodePoolsRolesChanging() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // volatile KNP which will be transitioned from mixed to -> controller only role and afterward to mixed role again
        final String volatileRolePoolName = testStorage.getMixedPoolName() + "-volatile";
        final String volatileSPSComponentName = KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), volatileRolePoolName);
        final LabelSelector volatilePoolLabelSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), volatileRolePoolName, ProcessRoles.CONTROLLER);


        // Stable KafkaNodePool for purpose of having at least 3 brokers and 3 controllers all the time.
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.mixedPoolPersistentStorage(testStorage.getNamespaceName(), volatileRolePoolName, testStorage.getClusterName(), 3).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );

        LOGGER.info("Create KafkaTopic {}/{} with 6 replicas, spawning across all brokers", testStorage.getNamespaceName(), testStorage.getTopicName());
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 1, 6).build();
        resourceManager.createResourceWithWait(kafkaTopic);

        LOGGER.info("wait for Kafka pods stability");
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), volatileSPSComponentName, 3);

        LOGGER.info("Start rolling update");
        Map<String, String> volatilePoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), volatilePoolLabelSelector);
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), volatileSPSComponentName, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasStartedRolling(testStorage.getNamespaceName(), volatilePoolLabelSelector, volatilePoolPodsSnapshot);

        LOGGER.info("Change role in {}/{}, from mixed to broker only resulting in revert", testStorage.getNamespaceName(), volatileRolePoolName);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), volatileRolePoolName, knp -> {
            knp.getSpec().setRoles(List.of(ProcessRoles.CONTROLLER));
        });

        LOGGER.info("Wait for warning message in Kafka {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), ".*Reverting role change.*");

        LOGGER.info("Wait for (original) rolling update to finish successfully");
        volatilePoolPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), volatilePoolLabelSelector, 3, volatilePoolPodsSnapshot);

        // remove topic which blocks role change (removal of broker role thus decreasing number of broker nodes available)
        LOGGER.info("Delete Kafka Topic {}/{}", testStorage.getNamespaceName(), testStorage.getTopicName());
        resourceManager.deleteResource(kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicDeletion(testStorage.getNamespaceName(), testStorage.getTopicName());

        // wait for final roll changing
        LOGGER.info("Wait for roll that will change role of KNP from mixed role to broker {}/{}", testStorage.getNamespaceName(), volatileRolePoolName);
        volatilePoolPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), volatilePoolLabelSelector, 3, volatilePoolPodsSnapshot);

        LOGGER.info("Change role in {}/{}, from broker only to mixed", testStorage.getNamespaceName(), volatileRolePoolName);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), volatileRolePoolName,
            knp -> knp.getSpec().setRoles(List.of(ProcessRoles.CONTROLLER, ProcessRoles.BROKER))
        );
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), volatilePoolLabelSelector, 3, volatilePoolPodsSnapshot);

        transmitMessagesWithNewTopicAndClean(testStorage, 5);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies the possibility of adding and removing KafkaNodePools into an existing Kafka cluster."),
        steps = {
            @Step(value = "Deploy a Kafka instance with annotations to manage KafkaNodePools and 2 initial KafkaNodePools.", expected = "Kafka instance is deployed according to Kafka and KafkaNodePool CustomResource."),
            @Step(value = "Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present.", expected = "KafkaTopic created."),
            @Step(value = "Deploy clients and transmit messages and remove KafkaTopic.", expected = "Transition of messages is finished successfully."),
            @Step(value = "Remove KafkaTopic.", expected = "KafkaTopic is cleaned as expected."),
            @Step(value = "Add extra KafkaNodePool with broker role to the Kafka.", expected = "KafkaNodePool is deployed and ready."),
            @Step(value = "Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present.", expected = "KafkaTopic created."),
            @Step(value = "Deploy clients and transmit messages and remove KafkaTopic.", expected = "Transition of messages is finished successfully."),
            @Step(value = "Remove KafkaTopic.", expected = "KafkaTopic is cleaned as expected."),
            @Step(value = "Remove one KafkaNodePool with broker role.", expected = "KafkaNodePool is removed, Pods are deleted, but other pods in Kafka are stable and ready."),
            @Step(value = "Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present.", expected = "KafkaTopic created."),
            @Step(value = "Deploy clients and transmit messages and remove KafkaTopic.", expected = "Transition of messages is finished successfully."),
            @Step(value = "Remove KafkaTopic.", expected = "KafkaTopic is cleaned as expected.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testNodePoolsAdditionAndRemoval() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // KafkaNodePools name convention is 'A' for all roles (: if possible i.e. based on feature gate) 'B' for broker roles.
        final String poolAName = testStorage.getBrokerPoolName() + "-a";
        final String poolB1Name = testStorage.getBrokerPoolName() + "-b1";
        final String poolB2NameAdded = testStorage.getBrokerPoolName() + "-b2-added";
        final int brokerNodePoolReplicaCount = 2;

        LOGGER.info("Deploy 2 KafkaNodePools {}, {}, in {}", poolAName, poolB1Name, testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), poolAName, testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), poolB1Name, testStorage.getClusterName(), brokerNodePoolReplicaCount).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
                .editOrNewSpec()
                    .editKafka()
                        .addToConfig("auto.create.topics.enable", "false")  // topics replica count helps ensure there are enough brokers
                        .addToConfig("offsets.topic.replication.factor", "3") // as some brokers (2) will be removed, this topic should have more than '1' default replica
                    .endKafka()
                .endSpec()
                .build()
        );

        transmitMessagesWithNewTopicAndClean(testStorage, 3);

        LOGGER.info("Add additional KafkaNodePool:  {}/{}", testStorage.getNamespaceName(), poolB2NameAdded);
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), poolB2NameAdded, testStorage.getClusterName(), brokerNodePoolReplicaCount).build()
        );

        KafkaNodePoolUtils.waitForKafkaNodePoolPodsReady(testStorage, poolB2NameAdded, ProcessRoles.BROKER, brokerNodePoolReplicaCount);

        // replica count of this KafkaTopic will require that new brokers were correctly added into Kafka cluster
        transmitMessagesWithNewTopicAndClean(testStorage, 5);

        LOGGER.info("Delete KafkaNodePool: {}/{} and wait for Kafka pods stability", testStorage.getNamespaceName(), poolB1Name);
        KafkaNodePoolUtils.deleteKafkaNodePoolWithPodSetAndWait(testStorage.getNamespaceName(), testStorage.getClusterName(), poolB1Name);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), poolB2NameAdded), brokerNodePoolReplicaCount);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), poolAName), 1);

        transmitMessagesWithNewTopicAndClean(testStorage, 2);
    }

    private void transmitMessagesWithNewTopicAndClean(TestStorage testStorage, int topicReplicas) {
        final String topicName = testStorage.getTopicName() + "-replicas-" + topicReplicas + "-" + hashStub(String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicName, testStorage.getClusterName(), 1, topicReplicas).build();
        resourceManager.createResourceWithWait(kafkaTopic);

        LOGGER.info("Transmit messages with Kafka {}/{} using topic {}", testStorage.getNamespaceName(), testStorage.getClusterName(), topicName);
        KafkaClients kafkaClients = ClientUtils.getInstantPlainClientBuilder(testStorage)
            .withTopicName(topicName)
            .build();
        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // clean topic
        resourceManager.deleteResource(kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicDeletion(testStorage.getNamespaceName(), topicName);
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .get()
            .withDefaultConfiguration()
            .install();
    }
}