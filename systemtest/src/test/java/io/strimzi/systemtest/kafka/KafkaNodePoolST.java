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
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

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

        KafkaNodePool kafkaNodePoolCr =  KafkaNodePoolTemplates.defaultKafkaNodePool(kafkaNodePoolName, testStorage.getClusterName(), 3)
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

    @ParallelNamespaceTest
    void testKafkaNodePoolBrokerIdsManagementUsingAnnotations(ExtensionContext extensionContext) {

        final TestStorage testStorage = new TestStorage(extensionContext);
        final int originalReplicaCount = 3;
        final int scaledReplicaCount = 6;
        String nodePoolAnnoIdsWithRange = "[20-21, 99]";

        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 2, 1)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .endMetadata()
            .build();

        KafkaNodePool kafkaNodePool =  KafkaNodePoolTemplates.defaultKafkaNodePool(testStorage.getKafkaNodePoolName(), testStorage.getClusterName(), 2)
            .editOrNewMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .addToRoles(ProcessRoles.BROKER)
                .withStorage(kafka.getSpec().getKafka().getStorage())
                .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                .withResources(kafka.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            kafkaNodePool,
            kafka);

        // Save for later comparison
        List<Integer> originalNodePoolIds = KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName());

        // Annotate NodePool for scale-up
        KafkaNodePoolUtils.annotateKafkaNodePoolNextNodeIds(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName(), nodePoolAnnoIdsWithRange);

        // Scale-up
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName(), scaledReplicaCount);
        KafkaNodePoolUtils.waitForKafkaNodePoolStablePodReplicasCount(testStorage, scaledReplicaCount);

        // Check correct IDS
        List<Integer> expectedNodePoolIds = KafkaNodePoolUtils.parseNodePoolIdWithRangeStringToIntegerList(nodePoolAnnoIdsWithRange);
        assertTrue(KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName()).containsAll(expectedNodePoolIds));

        // Annotate NodePool for scale-down
        KafkaNodePoolUtils.annotateKafkaNodePoolNextNodeIds(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName(), nodePoolAnnoIdsWithRange);
        // Scale-down
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName(), originalReplicaCount);
        KafkaNodePoolUtils.waitForKafkaNodePoolStablePodReplicasCount(testStorage, scaledReplicaCount);
        assertTrue(KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName()).containsAll(originalNodePoolIds));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+KafkaNodePools", null));

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(INFRA_NAMESPACE)
            .withExtraEnvVars(coEnvVars)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();
    }
}