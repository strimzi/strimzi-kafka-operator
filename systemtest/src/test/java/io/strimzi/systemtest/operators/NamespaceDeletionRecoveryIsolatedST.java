/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NamespaceUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.RECOVERY;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Suite for testing topic recovery in case of namespace deletion.
 * Procedure described in documentation  https://strimzi.io/docs/master/#namespace-deletion_str
 */
@Tag(RECOVERY)
class NamespaceDeletionRecoveryIsolatedST extends AbstractST {
    private String storageClassName = "retain";

    /**
     * In case that we have all KafkaTopic resources that existed before cluster loss, including internal topics,
     * we can simply recreate all KafkaTopic resources and then deploy the Kafka cluster.
     * At the end we verify that we can receive messages from topic (so data are present).
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(INTERNAL_CLIENTS_USED)
    void testTopicAvailable(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        prepareEnvironmentForRecovery(extensionContext, testStorage);

        // Wait till consumer offset topic is created
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix(testStorage.getNamespaceName(), "consumer-offsets");
        // Get list of topics and list of PVC needed for recovery
        List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).list().getItems();
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace();

        recreatePvcAndUpdatePv(persistentVolumeClaimList);
        recreateClusterOperator(extensionContext);

        // Recreate all KafkaTopic resources
        for (KafkaTopic kafkaTopic : kafkaTopicList) {
            kafkaTopic.getMetadata().setResourceVersion(null);
            KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).resource(kafkaTopic).create();
        }

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    /**
     * In case we don't have KafkaTopic resources from before the cluster loss, we do these steps:
     *  1. deploy the Kafka cluster without Topic Operator - otherwise topics will be deleted
     *  2. delete KafkaTopic Store topics - `__strimzi-topic-operator-kstreams-topic-store-changelog` and `__strimzi_store_topic`
     *  3. enable Topic Operator by redeploying Kafka cluster
     * @throws InterruptedException - sleep
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(INTERNAL_CLIENTS_USED)
    void testTopicNotAvailable(ExtensionContext extensionContext) throws InterruptedException {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        prepareEnvironmentForRecovery(extensionContext, testStorage);

        // Wait till consumer offset topic is created
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix(clusterOperator.getDeploymentNamespace(), "consumer-offsets");
        // Get list of topics and list of PVC needed for recovery
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace();
        recreatePvcAndUpdatePv(persistentVolumeClaimList);
        recreateClusterOperator(extensionContext);

        // Recreate Kafka Cluster
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        // Wait some time after kafka is ready before delete topics files
        Thread.sleep(60000);
        // Remove all topic data from topic store

        KafkaCmdClient.deleteTopicUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), "__strimzi-topic-operator-kstreams-topic-store-changelog");
        KafkaCmdClient.deleteTopicUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), "__strimzi_store_topic");

        // Wait till exec result will be finish
        Thread.sleep(30000);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().setEntityOperator(new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .withNewUserOperator()
                .endUserOperator().build());
        }, testStorage.getNamespaceName());

        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    private void prepareEnvironmentForRecovery(ExtensionContext extensionContext, TestStorage testStorage) {
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(testStorage.getNamespaceName())
            .createInstallation()
            .runInstallation();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    private void recreatePvcAndUpdatePv(List<PersistentVolumeClaim> persistentVolumeClaimList) {
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            pvc.getMetadata().setResourceVersion(null);
            kubeClient().createPersistentVolumeClaim(clusterOperator.getDeploymentNamespace(), pvc);

            PersistentVolume pv = kubeClient().getPersistentVolumeWithName(pvc.getSpec().getVolumeName());
            pv.getSpec().setClaimRef(null);
            kubeClient().updatePersistentVolume(pv);
        }
    }

    private void recreateClusterOperator(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(INFRA_NAMESPACE)
            .createInstallation()
            .runInstallation();
    }

    private void deleteAndRecreateNamespace() {
        // Delete namespace with all resources
        kubeClient().deleteNamespace(clusterOperator.getDeploymentNamespace());
        NamespaceUtils.waitForNamespaceDeletion(clusterOperator.getDeploymentNamespace());

        // Recreate namespace
        cluster.createNamespace(clusterOperator.getDeploymentNamespace());
        StUtils.copyImagePullSecrets(clusterOperator.getDeploymentNamespace());
    }

    @BeforeAll
    void createStorageClass() {
        kubeClient().getClient().storage().v1().storageClasses().withName(storageClassName).delete();
        StorageClass storageClass = new StorageClassBuilder()
            .withNewMetadata()
                .withName(storageClassName)
            .endMetadata()
            .withProvisioner("kubernetes.io/cinder")
            .withReclaimPolicy("Retain")
            .build();

        kubeClient().createStorageClass(storageClass);
    }

    @AfterAll
    void teardown() {
        kubeClient().deleteStorageClassWithName(storageClassName);

        kubeClient().getClient().persistentVolumes().list().getItems().stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains("kafka") || pv.getSpec().getClaimRef().getName().contains("zookeeper"))
            .forEach(pv -> kubeClient().getClient().persistentVolumes().resource(pv).delete());
    }
}
