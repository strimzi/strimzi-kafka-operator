/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import java.util.Arrays;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.extension.ExtensionContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        // Deploy Initial NodePool (which will hold initial topics and will never be scaled down) with IDs far from those that will be used in test
        final KafkaNodePool poolInitial = KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(nodePoolNameInitial, kafka, 2)
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

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());
        assumeTrue(Environment.isKRaftModeEnabled());

        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+UseKRaft,+KafkaNodePools", null));

        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withExtraEnvVars(coEnvVars)
            .createInstallation()
            .runInstallation();
    }
}