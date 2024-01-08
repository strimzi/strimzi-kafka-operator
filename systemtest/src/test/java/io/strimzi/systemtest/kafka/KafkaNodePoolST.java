/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;


import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.operator.common.Util.hashStub;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;


@Tag(REGRESSION)
public class KafkaNodePoolST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaNodePoolST.class);

    /**
     * @description This test case verifies the management of broker IDs in Kafka Node Pools using annotations.
     *
     * @steps
     *  1. - Deploy a Kafka instance with annotations to manage Node Pools and Initial NodePool (Initial) to hold Topics and act as controller.
     *     - Kafka instance is deployed according to Kafka and KafkaNodePool custom resource, with IDs 90, 91.
     *  2. - Deploy additional 2 NodePools (A,B) with 1 and 2 replicas, and preset 'next-node-ids' annotations holding resp. values ([4],[6]).
     *     - NodePools are deployed, NodePool A contains ID 4, NodePoolB contains Ids 6, 0.
     *  3. - Annotate NodePool A 'next-node-ids' and NodePool B 'remove-node-ids' respectively ([20-21],[6,55]) afterward scale to 4 and 1 replica resp.
     *     - NodePools are scaled, NodePool A contains IDs 4, 20, 21, 1. NodePool B contains ID 0.
     *  4. - Annotate NodePool A 'remove-node-ids' and NodePool B 'next-node-ids' respectively ([20],[1]) afterward scale to 2 and 6 replica resp.
     *     - NodePools are scaled, NodePool A contains IDs 1, 4. NodePool B contains ID 2, 3, 5.
     *
     * @usecase
     *  - kafka-node-pool
     *  - broker-id-management
     */
    @ParallelNamespaceTest
    void testKafkaNodePoolBrokerIdsManagementUsingAnnotations(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String nodePoolNameA = testStorage.getKafkaNodePoolName() + "-a";
        final String nodePoolNameB = testStorage.getKafkaNodePoolName() + "-b";
        final String nodePoolNameInitial = testStorage.getKafkaNodePoolName() + "-initial";

        final Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1).build();

        // Deploy Initial NodePool (which will hold initial topics and will never be scaled down) with IDs far from those that will be used in the test
        final KafkaNodePool poolInitial = KafkaNodePoolTemplates.kafkaBasedNodePoolWithFgBasedRole(nodePoolNameInitial, kafka, 2)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[91-93]"))
            .endMetadata()
            .build();
        resourceManager.createResourceWithWait(extensionContext, poolInitial, kafka);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameInitial), 2);

        LOGGER.info("Testing deployment of NodePools with pre-configured annotation: {} is creating Brokers with correct IDs", Annotations.ANNO_STRIMZI_IO_NODE_POOLS);

        // Deploy NodePool A with only 1 replica and next ID 4, and NodePool B with 2 replica and next ID 6
        final KafkaNodePool poolA = KafkaNodePoolTemplates.kafkaBasedNodePoolWithBrokerRole(nodePoolNameA, kafka, 1)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[4]"))
            .endMetadata()
            .build();
        final KafkaNodePool poolB = KafkaNodePoolTemplates.kafkaBasedNodePoolWithBrokerRole(nodePoolNameB, kafka, 2)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[6]"))
            .endMetadata()
            .build();
        resourceManager.createResourceWithWait(extensionContext, poolA, poolB);
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

    /**
     * @description This test case verifies possibility of adding and removing Kafka Node Pools into existing Kafka cluster.
     *
     * @steps
     *  1. - Deploy a Kafka instance with annotations to manage Node Pools and Initial 2 NodePools, one being controller if possible other initial broker.
     *     - Kafka instance is deployed according to Kafka and KafkaNodePool custom resource.
     *  2. - Create KafkaTopic with replica number requiring all Kafka Brokers to be present, Deploy clients and transmit messages and remove KafkaTopic.
     *     - transition of messages is finished successfully, KafkaTopic created and cleaned as expected.
     *  3. - Add extra KafkaNodePool with broker role to the Kafka.
     *     - KafkaNodePool is deployed and ready.
     *  4. - Create KafkaTopic with replica number requiring all Kafka Brokers to be present, Deploy clients and transmit messages and remove KafkaTopic.
     *     - transition of messages is finished successfully, KafkaTopic created and cleaned as expected.
     *  5. - Remove one of kafkaNodePool with broker role.
     *     - KafkaNodePool is removed, Pods are deleted, but other pods in Kafka are stable and ready.
     *  6. - Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present, Deploy clients and transmit messages and remove KafkaTopic.
     *     - transition of messages is finished successfully, KafkaTopic created and cleaned as expected.
     *
     * @usecase
     *  - kafka-node-pool
     */
    @ParallelNamespaceTest
    void testNodePoolsAdditionAndRemoval(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        // node pools name convention is 'A' for all roles (: if possible i.e. based on feature gate) 'B' for broker roles.
        final String poolAName = testStorage.getKafkaNodePoolName() + "-a";
        final String poolB1Name = testStorage.getKafkaNodePoolName() + "-b1";
        final String poolB2NameAdded = testStorage.getKafkaNodePoolName() + "-b2-added";
        final int brokerNodePoolReplicaCount = 2;

        LOGGER.info("Deploy 2 KafkaNodePools {}, {}, in {}", poolAName, poolB1Name, testStorage.getNamespaceName());
        final Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 3)
            .editOrNewSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")  // topics replica count helps ensure there are enough brokers
                    .addToConfig("offsets.topic.replication.factor", "3") // as some brokers (2) will be removed, this topic should have more than '1' default replica
                .endKafka()
            .endSpec()
            .build();
        final KafkaNodePool poolA = KafkaNodePoolTemplates.kafkaBasedNodePoolWithFgBasedRole(poolAName, kafka, 1).build();
        final KafkaNodePool poolB1 = KafkaNodePoolTemplates.kafkaBasedNodePoolWithBrokerRole(poolB1Name, kafka, brokerNodePoolReplicaCount).build();
        resourceManager.createResourceWithWait(extensionContext, poolA, poolB1, kafka);

        transmitMessagesWithNewTopicAndClean(testStorage, 3);

        LOGGER.info("Add additional KafkaNodePool:  {}/{}", testStorage.getNamespaceName(), poolB2NameAdded);
        final KafkaNodePool poolB2Added = KafkaNodePoolTemplates.kafkaBasedNodePoolWithBrokerRole(poolB2NameAdded, kafka, brokerNodePoolReplicaCount).build();
        resourceManager.createResourceWithWait(extensionContext, poolB2Added);
        KafkaNodePoolUtils.waitForKafkaNodePoolPodsReady(testStorage, poolB2NameAdded, ProcessRoles.BROKER, brokerNodePoolReplicaCount);

        // replica count of this KafkaTopic will require that new brokers were correctly added into Kafka Cluster
        transmitMessagesWithNewTopicAndClean(testStorage, 5);

        LOGGER.info("Delete KafkaNodePool: {}/{} and wait for Kafka pods stability", testStorage.getNamespaceName(), poolB1Name);
        KafkaNodePoolUtils.deleteKafkaNodePoolWithPodSetAndWait(testStorage.getNamespaceName(), testStorage.getClusterName(), poolB1Name);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), poolB2NameAdded), brokerNodePoolReplicaCount);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), poolAName), 1);

        transmitMessagesWithNewTopicAndClean(testStorage, 2);
    }

    private void transmitMessagesWithNewTopicAndClean(TestStorage testStorage, int topicReplicas) {
        final String topicName = testStorage.getTopicName() + "-replicas-" + topicReplicas + "-" + hashStub(String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(testStorage.getClusterName(), topicName, 1, topicReplicas, testStorage.getNamespaceName()).build();
        resourceManager.createResourceWithWait(testStorage.getExtensionContext(), kafkaTopic);

        LOGGER.info("Transmit messages with Kafka {}/{} using topic {}", testStorage.getNamespaceName(), testStorage.getClusterName(), topicName);
        KafkaClients kafkaClients = ClientUtils.getDefaultClientBuilder(testStorage)
            .withTopicName(topicName)
            .build();
        resourceManager.createResourceWithWait(testStorage.getExtensionContext(),
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        resourceManager.deleteResource(kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicDeletion(testStorage.getNamespaceName(), topicName);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());
        assumeTrue(Environment.isKafkaNodePoolsEnabled());

        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}