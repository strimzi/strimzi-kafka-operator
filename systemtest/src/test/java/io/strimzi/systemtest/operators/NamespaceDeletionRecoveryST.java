/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.RECOVERY;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Suite for testing topic recovery in case of namespace deletion.
 * Procedure described in documentation  https://strimzi.io/docs/master/#namespace-deletion_str
 * Note: Suite can be run on minikube only with previously created PVs and StorageClass using local provisioner.
 * Reason why this test class is not part of regression:
 * These tests does not have to be run every time with PRs and so on, the nature of the tests is sufficient for recovery profile only.
 */
@Tag(RECOVERY)
class NamespaceDeletionRecoveryST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(NamespaceDeletionRecoveryST.class);
    private String storageClassName = "retain";

    /**
     * In case that we have all KafkaTopic resources that existed before cluster loss, including internal topics,
     * we can simply recreate all KafkaTopic resources and then deploy the Kafka cluster.
     * At the end we verify that we can receive messages from topic (so data are present).
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testTopicAvailable() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        prepareEnvironmentForRecovery(testStorage);

        // Get list of topics and list of PVC needed for recovery
        List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).list().getItems();
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace(testStorage.getNamespaceName());

        recreatePvcAndUpdatePv(testStorage.getNamespaceName(), persistentVolumeClaimList);
        recreateClusterOperator(testStorage.getNamespaceName());

        // Recreate all KafkaTopic resources
        for (KafkaTopic kafkaTopic : kafkaTopicList) {
            kafkaTopic.getMetadata().setResourceVersion(null);
            KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).resource(kafkaTopic).create();
        }

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build()
        );

        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                    .endPersistentClaimStorage()
                .endKafka()
            .endSpec()
            .build());

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    /**
     * In case we don't have KafkaTopic resources from before the cluster loss, we do these steps:
     *  1. deploy the Kafka cluster without Topic Operator - otherwise topics will be deleted
     *  2. enable Topic Operator by redeploying Kafka cluster
     *
     **/
    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testTopicNotAvailable() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        prepareEnvironmentForRecovery(testStorage);

        // Get list of topics and list of PVC needed for recovery
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("List of PVCs inside Namespace: {}", testStorage.getNamespaceName());
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            PersistentVolume pv = kubeClient().getPersistentVolumeWithName(pvc.getSpec().getVolumeName());
            LOGGER.info("Claim: {} has bounded Volume: {}", pvc.getMetadata().getName(), pv.getMetadata().getName());
        }

        // store cluster-id from Kafka CR
        final String oldClusterId = KafkaResource.kafkaClient()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName())
            .get()
            .getStatus()
            .getClusterId();

        final String kafkaPodName = kubeClient().listPods(testStorage.getNamespaceName(),
            KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), testStorage.getBrokerPoolName(), ProcessRoles.BROKER)).get(0).getMetadata().getName();

        LOGGER.info("Currently present Topics inside Kafka: {}/{} are: {}", testStorage.getNamespaceName(), kafkaPodName,
            KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), kafkaPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName())));

        LOGGER.info("Deleting namespace and recreating for recovery");
        deleteAndRecreateNamespace(testStorage.getNamespaceName());

        LOGGER.info("Recreating PVCs and updating PVs for recovery");
        recreatePvcAndUpdatePv(testStorage.getNamespaceName(), persistentVolumeClaimList);

        LOGGER.info("Recreating Cluster Operator");
        recreateClusterOperator(testStorage.getNamespaceName());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaNodePoolResource.getBrokerPoolName(testStorage.getClusterName()), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaNodePoolResource.getControllerPoolName(testStorage.getClusterName()), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build()
        );

        LOGGER.info("Recreating Kafka cluster without Topic Operator");
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editMetadata()
                // Deploy the Kafka cluster using the original configuration for the Kafka resource.
                // Add the annotation strimzi.io/pause-reconciliation="true" to the original configuration for the Kafka resource,
                // and then deploy the Kafka cluster using the updated configuration.
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true")
            .endMetadata()
            .editSpec()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .build()
        );

        // Edit the Kafka resource to set the .status.clusterId with the recovered value
        KafkaResource.kafkaClient()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName())
            .subresource("status")
            .patch(PatchContext.of(PatchType.JSON_MERGE), String.format("{\"status\": {\"clusterId\": \"%s\"}}", oldClusterId));

        //  Unpause the Kafka resource reconciliation
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            kafka -> {
                Map<String, String> annotations = kafka.getMetadata().getAnnotations();
                annotations.put(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false");
                kafka.getMetadata().setAnnotations(annotations);
            });

        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Currently present Topics inside Kafka: {}/{} are: {}", testStorage.getNamespaceName(), kafkaPodName,
            KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), kafkaPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName())));

        LOGGER.info("Adding Topic Operator to existing Kafka");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().setEntityOperator(new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .withNewUserOperator()
                .endUserOperator().build());
        });

        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1);

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    private void prepareEnvironmentForRecovery(TestStorage testStorage) {
        LOGGER.info("####################################");
        LOGGER.info("Creating environment for recovery");
        LOGGER.info("####################################");
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(testStorage.getNamespaceName())
                .build()
            )
            .install();

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build()
        );

        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("##################################################");
        LOGGER.info("Environment for recovery was successfully created");
        LOGGER.info("##################################################");
    }


    private void recreatePvcAndUpdatePv(String namespaceName, List<PersistentVolumeClaim> persistentVolumeClaimList) {
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            pvc.getMetadata().setResourceVersion(null);
            kubeClient().createPersistentVolumeClaim(namespaceName, pvc);

            PersistentVolume pv = kubeClient().getPersistentVolumeWithName(pvc.getSpec().getVolumeName());
            pv.getSpec().setClaimRef(null);
            kubeClient().updatePersistentVolume(pv);

            PersistentVolumeClaimUtils.waitForPersistentVolumeClaimPhase(pv.getMetadata().getName(), TestConstants.PVC_PHASE_BOUND);
        }
    }

    private void recreateClusterOperator(String namespace) {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(namespace)
                .build()
            )
            .install();
    }

    private void deleteAndRecreateNamespace(String namespace) {
        NamespaceManager.getInstance().deleteNamespaceWithWait(namespace);

        // Recreate namespace
        NamespaceManager.getInstance().createNamespaceAndPrepare(namespace);
    }

    @BeforeAll
    void createStorageClass() {
        // Delete specific StorageClass if present from previous
        kubeClient().getClient().storage().v1().storageClasses().withName(storageClassName).delete();

        final String storageClassKubernetesIo = "storageclass.kubernetes.io/is-default-class";
        // Get default StorageClass and change reclaim policy
        StorageClass defaultStorageClass =  kubeClient().getClient().storage().v1().storageClasses().list().getItems().stream().filter(sg -> {
            Map<String, String> annotations = sg.getMetadata().getAnnotations();
            return annotations != null && annotations.containsKey(storageClassKubernetesIo) && annotations.get(storageClassKubernetesIo).equals("true");
        }).findFirst().get();

        StorageClass retainStorageClass = new StorageClassBuilder(defaultStorageClass)
            .withNewMetadata()
                .withName(storageClassName)
            .endMetadata()
            .withReclaimPolicy("Retain")
            .withVolumeBindingMode("WaitForFirstConsumer")
            .build();

        kubeClient().createStorageClass(retainStorageClass);
    }

    @AfterAll
    void teardown() {
        kubeClient().deleteStorageClassWithName(storageClassName);

        kubeClient().getClient().persistentVolumes().list().getItems().stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains("kafka"))
            .forEach(pv -> kubeClient().getClient().persistentVolumes().resource(pv).delete());
    }
}
