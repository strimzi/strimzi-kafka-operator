/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.recovery;

import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NamespaceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static io.strimzi.systemtest.Constants.RECOVERY;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(RECOVERY)
class NamespaceDeletionRecoveryST extends BaseST {

    static final String NAMESPACE = "namespace-recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(NamespaceDeletionRecoveryST.class);

    private String storageClassName = "retain";

    @Test
    void testTopicAvailable() throws InterruptedException {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);

        prepareEnvironmentForRecovery(topicName, MESSAGE_COUNT);

        // Wait till consumer offset topic is created
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix("consumer-offsets");
        // Get list of topics and list of PVC needed for recovery
        List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).list().getItems();
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace();

        recreatePvcAndUpdatePv(persistentVolumeClaimList);
        recreateClusterOperator();

        // Recreate all KafkaTopic resources
        for (KafkaTopic kafkaTopic : kafkaTopicList) {
            kafkaTopic.getMetadata().setResourceVersion(null);
            KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).createOrReplace(kafkaTopic);
        }

        String consumerGroup = "group-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec().done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        Integer consumed = internalKafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT, consumerGroup);
        assertThat(consumed, is(MESSAGE_COUNT));
    }

    @Test
    void testTopicNotAvailable() throws InterruptedException {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);

        prepareEnvironmentForRecovery(topicName, MESSAGE_COUNT);

        // Wait till consumer offset topic is created
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix("consumer-offsets");
        // Get list of topics and list of PVC needed for recovery
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace();
        recreatePvcAndUpdatePv(persistentVolumeClaimList);
        recreateClusterOperator();

        String consumerGroup = "group-" + new Random().nextInt(Integer.MAX_VALUE);
        // Recreate Kafka Cluster
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec().done();

        // Wait some time after kafka is ready before delete topics files
        Thread.sleep(60000);
        // Remove all topic data from zookeeper
        String deleteZkDataCmd = "sh /opt/kafka/bin/zookeeper-shell.sh localhost:2181 <<< \"deleteall /strimzi\"";
        cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", deleteZkDataCmd);
        // Wait till exec result will be finish
        Thread.sleep(30000);
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().setEntityOperator(new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .withNewUserOperator()
                .endUserOperator().build());
        });

        DeploymentUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        Integer consumed = internalKafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT, consumerGroup);
        assertThat(consumed, is(MESSAGE_COUNT));
    }

    private void prepareEnvironmentForRecovery(String topicName, int messageCount) {
        String consumerGroup = "group-" + new Random().nextInt(Integer.MAX_VALUE);
        // Setup Test environment with Kafka and store some messages
        prepareEnvForOperator(NAMESPACE);
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec().done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessages(topicName, NAMESPACE, CLUSTER_NAME, messageCount),
                internalKafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, messageCount, consumerGroup)
        );
    }

    private void recreatePvcAndUpdatePv(List<PersistentVolumeClaim> persistentVolumeClaimList) {
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            pvc.getMetadata().setResourceVersion(null);
            kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).create(pvc);

            PersistentVolume pv = kubeClient().getClient().persistentVolumes().withName(pvc.getSpec().getVolumeName()).get();
            pv.getSpec().setClaimRef(null);
            kubeClient().getClient().persistentVolumes().createOrReplace(pv);
        }
    }

    private void recreateClusterOperator() {
        // Recreate CO
        cluster.applyClusterOperatorInstallFiles();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }

    private void deleteAndRecreateNamespace() {
        // Delete namespace with all resources
        kubeClient().deleteNamespace(NAMESPACE);
        NamespaceUtils.waitForNamespaceDeletion(NAMESPACE);

        // Recreate namespace
        cluster.createNamespace(NAMESPACE);
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) { }

    @BeforeAll
    void createStorageClass() {
        kubeClient().getClient().storage().storageClasses().inNamespace(NAMESPACE).withName(storageClassName).delete();
        StorageClass storageClass = new StorageClassBuilder()
            .withNewMetadata()
                .withName(storageClassName)
            .endMetadata()
            .withProvisioner("kubernetes.io/cinder")
            .withReclaimPolicy("Retain")
            .build();

        kubeClient().getClient().storage().storageClasses().inNamespace(NAMESPACE).createOrReplace(storageClass);
    }

    @AfterAll
    void teardown() {
        kubeClient().getClient().storage().storageClasses().inNamespace(NAMESPACE).withName(storageClassName).delete();

        kubeClient().getClient().persistentVolumes().list().getItems().stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains("kafka") || pv.getSpec().getClaimRef().getName().contains("zookeeper"))
            .forEach(pv -> kubeClient().getClient().persistentVolumes().delete(pv));
    }
}
