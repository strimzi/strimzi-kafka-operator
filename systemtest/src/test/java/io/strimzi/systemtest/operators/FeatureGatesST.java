/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
public class FeatureGatesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesST.class);

    /**
     * @description This test case verifies basic working of Kafka Cluster managed by Cluster Operator with KRaft feature gate enabled.
     *
     * @steps
     *  1. - Deploy Kafka annotated to enable Kraft (and additionally annotated to enable management by KafkaNodePool due to default usage of NodePools), and KafkaNodePool targeting given Kafka Cluster.
     *     - Kafka is deployed, KafkaNodePool custom resource is targeting Kafka Cluster as expected.
     *  2. - Produce and consume messages in given Kafka Cluster.
     *     - Clients can produce and consume messages.
     *  3. - Trigger manual Rolling Update.
     *     - Rolling update is triggered and completed shortly after.
     *
     * @usecase
     *  - kafka-node-pool
     *  - kraft
     */
    @IsolatedTest("Feature Gates test for enabled UseKRaft gate")
    @Tag(INTERNAL_CLIENTS_USED)
    public void testKRaftMode() {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int kafkaReplicas = 3;

        // as kraft is included in CO Feature gates, kafka broker can take both roles (Controller and Broker)
        setupClusterOperatorWithFeatureGate("+UseKRaft");

        final Kafka kafkaCr = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), kafkaReplicas)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build();

        kafkaCr.getSpec().getEntityOperator().setTopicOperator(null); // The builder cannot disable the EO. It has to be done this way.

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), kafkaReplicas).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), kafkaReplicas).build(),
            kafkaCr
        );

        // Check that there is no ZooKeeper
        Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(),
            KafkaResource.getLabelSelector(testStorage.getClusterName(), KafkaResources.zookeeperComponentName(testStorage.getClusterName())));
        assertThat("No ZooKeeper Pods should exist", zkPods.size(), is(0));

        // create KafkaTopic with replication factor on all brokers and min.insync replicas configuration to not loss data during Rolling Update.
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getContinuousTopicName(), 1, kafkaReplicas, kafkaReplicas - 1, testStorage.getNamespaceName()).build());

        KafkaClients clients = ClientUtils.getContinuousPlainClientBuilder(testStorage).build();
        LOGGER.info("Producing and Consuming messages with continuous clients: {}, {} in Namespace {}", testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );

        // Roll Kafka
        LOGGER.info("Forcing rolling update of Kafka via read-only configuration change");
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().getConfig().put("log.retention.hours", 72), testStorage.getNamespaceName());

        LOGGER.info("Waiting for the next reconciliation to happen");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector(), kafkaReplicas, brokerPods);

        LOGGER.info("Waiting for clients to finish sending/receiving messages");
        ClientUtils.waitForContinuousClientSuccess(testStorage);
    }

    /**
     * @description This test case verifies transfer of Kafka Cluster from and to management by KafkaNodePool, by creating corresponding Kafka and KafkaNodePool custom resources
     * and manipulating according kafka annotation.
     *
     * @steps
     * 1. - Deploy Kafka with annotated to enable management by KafkaNodePool, and KafkaNodePool targeting given Kafka Cluster.
     *    - Kafka is deployed, KafkaNodePool custom resource is targeting Kafka Cluster as expected.
     * 2. - Modify KafkaNodePool by increasing number of Kafka Replicas.
     *    - Number of Kafka Pods is increased to match specification from KafkaNodePool
     * 3. - Produce and consume messages in given Kafka Cluster.
     *    - Clients can produce and consume messages.
     * 4. - Modify Kafka custom resource annotation strimzi.io/node-pool to disable management by KafkaNodePool.
     *    - StrimziPodSet is modified, replacing former one, Pods are replaced and specification from KafkaNodePool (i.e., changed replica count) are ignored.
     * 5. - Produce and consume messages in given Kafka Cluster.
     *    - Clients can produce and consume messages.
     * 6. - Modify Kafka custom resource annotation strimzi.io/node-pool to enable management by KafkaNodePool.
     *    - new StrimziPodSet is created, replacing former one, Pods are replaced and specification from KafkaNodePool (i.e., changed replica count) has priority over Kafka specification.
     * 7. - Produce and consume messages in given Kafka Cluster.
     *    - Clients can produce and consume messages.
     *
     * @usecase
     * - kafka-node-pool
     */
    @IsolatedTest
    void testKafkaManagementTransferToAndFromKafkaNodePool() {
        assumeFalse(Environment.isKRaftModeEnabled());
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int originalKafkaReplicaCount = 3;
        final int nodePoolIncreasedKafkaReplicaCount = 5;
        final String kafkaNodePoolName = "kafka";

        // as the only FG set in the CO is 'KafkaNodePools' (kraft not included) Broker role is the only one that kafka broker can take
        setupClusterOperatorWithFeatureGate("");

        LOGGER.info("Deploying Kafka Cluster: {}/{} controlled by KafkaNodePool: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaNodePoolName);

        final Kafka kafkaCr = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), originalKafkaReplicaCount, 3)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build();

        // as the only FG set in the CO is 'KafkaNodePools' (kraft is never included) Broker role is the only one that can be taken
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), kafkaNodePoolName, testStorage.getClusterName(), 3).build(),
            kafkaCr);

        LOGGER.info("Creating KafkaTopic: {}/{}", testStorage.getNamespaceName(), testStorage.getTopicName());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // increase number of kafka replicas in KafkaNodePool
        LOGGER.info("Modifying KafkaNodePool: {}/{} by increasing number of Kafka replicas from '3' to '5'", testStorage.getNamespaceName(), kafkaNodePoolName);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(kafkaNodePoolName, kafkaNodePool -> {
            kafkaNodePool.getSpec().setReplicas(nodePoolIncreasedKafkaReplicaCount);
        }, testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(
            testStorage.getNamespaceName(),
            testStorage.getClusterName(),
            KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), kafkaNodePoolName),
            nodePoolIncreasedKafkaReplicaCount
        );

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Disable KafkaNodePool in Kafka Cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> {
                kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "disabled");
                // because Kafka CR with NodePools is missing .spec.kafka.replicas and .spec.kafka.storage, we need to
                // set those here
                kafka.getSpec().getKafka().setReplicas(originalKafkaReplicaCount);
                kafka.getSpec().getKafka().setStorage(new PersistentClaimStorageBuilder()
                    .withSize("1Gi")
                    .withDeleteClaim(true)
                    .build()
                );
            }, testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(
            testStorage.getNamespaceName(),
            testStorage.getClusterName(),
            KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), kafkaNodePoolName),
            originalKafkaReplicaCount
        );
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResources.kafkaComponentName(testStorage.getClusterName()), originalKafkaReplicaCount);

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Enable KafkaNodePool in Kafka Cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> {
                kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled");
                kafka.getSpec().getKafka().setReplicas(null);
                kafka.getSpec().getKafka().setStorage(null);
            }, testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(
            testStorage.getNamespaceName(),
            testStorage.getClusterName(),
            KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), kafkaNodePoolName),
            nodePoolIncreasedKafkaReplicaCount
        );
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResources.kafkaComponentName(testStorage.getClusterName()), nodePoolIncreasedKafkaReplicaCount);

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    /**
     * Sets up a Cluster Operator with specified feature gates.
     *
     * @param extraFeatureGates A String representing additional feature gates (comma separated) to be
     *                          enabled or disabled for the Cluster Operator.
     */
    private void setupClusterOperatorWithFeatureGate(String extraFeatureGates) {
        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, extraFeatureGates, null));

        clusterOperator = this.clusterOperator.defaultInstallation()
            .withExtraEnvVars(coEnvVars)
            // necessary as each isolated test removes TEST_SUITE_NAMESPACE
            .withBindingsNamespaces(Arrays.asList(TestConstants.CO_NAMESPACE, Environment.TEST_SUITE_NAMESPACE))
            .createInstallation()
            .runInstallation();
    }
}
