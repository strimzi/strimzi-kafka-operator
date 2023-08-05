/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;


import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import java.util.Arrays;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tag(REGRESSION)
public class KafkaNodePoolST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaNodePoolST.class);

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
    @ParallelNamespaceTest
    void testKafkaManagementTransferToAndFromKafkaNodePool(ExtensionContext extensionContext) {

        final TestStorage testStorage = new TestStorage(extensionContext);

        final int originalKafkaReplicaCount = 3;
        final int nodePoolIncreasedKafkaReplicaCount = 5;
        final String kafkaNodePoolName = "kafka";

        // setup clients
        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withDelayMs(200)
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        Map<String, String> coPod = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());

        LOGGER.info("Deploying Kafka Cluster: {}/{} controlled by KafkaNodePool: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaNodePoolName);

        Kafka kafkaCr = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), originalKafkaReplicaCount, 1)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .endMetadata()
            .build();

        KafkaNodePool kafkaNodePoolCr =  KafkaNodePoolTemplates.defaultKafkaNodePool(testStorage.getNamespaceName(), kafkaNodePoolName, testStorage.getClusterName(), 3)
            .editOrNewMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .addToRoles(ProcessRoles.BROKER)
                .withStorage(kafkaCr.getSpec().getKafka().getStorage())
                .withJvmOptions(kafkaCr.getSpec().getKafka().getJvmOptions())
                .withResources(kafkaCr.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            kafkaNodePoolCr,
            kafkaCr);

        LOGGER.info("Creating KafkaTopic: {}/{}", testStorage.getNamespaceName(), testStorage.getTopicName());
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        // increase number of kafka replicas in KafkaNodePool
        LOGGER.info("Modifying KafkaNodePool: {}/{} by increasing number of Kafka replicas from '3' to '5'", testStorage.getNamespaceName(), kafkaNodePoolName);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(kafkaNodePoolName, kafkaNodePool -> {
            kafkaNodePool.getSpec().setReplicas(nodePoolIncreasedKafkaReplicaCount);
        }, testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(
            testStorage.getNamespaceName(),
            KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), kafkaNodePoolName),
            KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()),
            nodePoolIncreasedKafkaReplicaCount
        );

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Changing FG env variable to disable Kafka Node Pools");
        List<EnvVar> coEnvVars = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME).getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        coEnvVars.stream().filter(env -> env.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV)).findFirst().get().setValue("-KafkaNodePools");

        Deployment coDep = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME);
        coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(coEnvVars);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coDep).update();

        coPod = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME, 1, coPod);

        LOGGER.info("Disable KafkaNodePool in Kafka Cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> {
                kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "disabled");
            }, testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(
            testStorage.getNamespaceName(),
            KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()),
            KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()),
            originalKafkaReplicaCount
        );
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()), originalKafkaReplicaCount);

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );

        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Changing FG env variable to enable Kafka Node Pools");
        coEnvVars = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME).getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        coEnvVars.stream().filter(env -> env.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV)).findFirst().get().setValue("+KafkaNodePools");

        coDep = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME);
        coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(coEnvVars);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coDep).update();

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME, 1, coPod);

        LOGGER.info("Enable KafkaNodePool in Kafka Cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> {
                kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled");
            }, testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(
            testStorage.getNamespaceName(),
            KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), kafkaNodePoolName),
            KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()),
            nodePoolIncreasedKafkaReplicaCount
        );
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()), nodePoolIncreasedKafkaReplicaCount);

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

    }

    /**
     * @description This test case verifies KafkaNodePools scaling up and down with correct NodePool IDs, with different NodePools.
     * @param extensionContext
     */
    @ParallelNamespaceTest
    void testKafkaNodePoolBrokerIdsManagementUsingAnnotations(ExtensionContext extensionContext) {

        final TestStorage testStorage = new TestStorage(extensionContext);
        String nodePoolNameA = testStorage.getKafkaNodePoolName() + "-a";
        String nodePoolNameB = testStorage.getKafkaNodePoolName() + "-b";

        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .endMetadata()
            .build();

        LOGGER.info("Testing deployment of NodePools with pre-configured annotation: {} is creating Brokers with correct IDs", Annotations.ANNO_STRIMZI_IO_NODE_POOLS);
        // Deploy NodePool A with only 1 replica and give it annotation with 1 ID
        KafkaNodePool poolA =  KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRole(testStorage.getNamespaceName(), nodePoolNameA, testStorage.getClusterName(), 1)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[5]"))
            .endMetadata()
            .editOrNewSpec()
                .withStorage(kafka.getSpec().getKafka().getStorage())
                .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                .withResources(kafka.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        // Deploy NodePool B with 2 replicas and give it annotation with only  1 ID
        KafkaNodePool poolB =  KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRole(testStorage.getNamespaceName(), nodePoolNameB, testStorage.getClusterName(), 2)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[6]"))
            .endMetadata()
            .editOrNewSpec()
                .withStorage(kafka.getSpec().getKafka().getStorage())
                .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                .withResources(kafka.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, poolA, poolB, kafka);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 1);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 2);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [5]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).get(0).equals(5));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0, 6]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).equals(Arrays.asList(0, 6)));

        LOGGER.info("Testing annotation with upscaling NodePool A (more replicas than specified IDs) " +
                    "and downscaling NodePool B (more IDs than needed to be scaled down. This redundant ID is not present)");
        // Annotate NodePool A for scale up with fewer IDs than needed -> this should cause addition of non-used ID starting from [0] in ASC order, which is in this case [1]
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameA, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS,  "[20-21]"));
        // Annotate NodePool B for scale down with more IDs than needed - > this should not matter as ID [99] is not present so only ID [6] is removed
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameB, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[6, 99]"));
        // Scale NodePool A up + NodePool B down
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, 4);
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, 1);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 4);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 1);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [1, 5, 20, 21]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).equals(Arrays.asList(1, 5, 20, 21)));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).get(0).equals(0));

        // 3. Case (A-missing ID for downscale, B-already used ID for upscale)
        LOGGER.info("Testing annotation with downscaling NodePool A (fewer IDs than needed) and NodePool B (already used ID)");
        // Annotate NodePool A for scale down with fewer IDs than needed, this should cause removal of IDs in DESC order after the annotated ID is deleted
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameA, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS,  "[20]"));
        // Annotate NodePool B for scale up with ID [1] already in use
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameB, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1]"));
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, 2);
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, 6);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 2);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 6);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [1, 5]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).equals(Arrays.asList(1, 5)));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0, 2, 3, 4, 6, 7]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).equals(Arrays.asList(0, 2, 3, 4, 6, 7)));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+KafkaNodePools", null));

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.CO_NAMESPACE)
            .withExtraEnvVars(coEnvVars)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();
    }
}