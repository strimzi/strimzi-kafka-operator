/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;
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

@Tag(RECOVERY)
@SuiteDoc(
    description = @Desc("Test suite for verifying Kafka cluster recovery after namespace deletion. Tests cover scenarios with and without `KafkaTopic` resources available, using persistent volumes with the `Retain` reclaim policy. Note: This suite requires `StorageClass` with a local provisioner on Minikube."),
    beforeTestSteps = {
        @Step(value = "Create a `StorageClass` with the `Retain` reclaim policy.", expected = "`StorageClass` is created with `WaitForFirstConsumer` volume binding mode.")
    },
    afterTestSteps = {
        @Step(value = "Clean up orphaned persistent volumes.", expected = "All Kafka-related persistent volumes are deleted.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
class NamespaceDeletionRecoveryST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(NamespaceDeletionRecoveryST.class);
    private final String storageClassName = "retain";

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @TestDoc(
        description = @Desc("This test verifies Kafka cluster recovery when all `KafkaTopic` resources are available after namespace deletion, including internal topics. The test recreates `KafkaTopic` resources first, then deploys the Kafka cluster and verifies that existing data is preserved."),
        steps = {
            @Step(value = "Prepare the test environment with a Kafka cluster and `KafkaTopic`.", expected = "The Kafka cluster is deployed with persistent storage, and the topic contains test data."),
            @Step(value = "Store the list of all `KafkaTopic` and `PersistentVolumeClaim` resources.", expected = "All `KafkaTopic` and `PersistentVolumeClaim`  resources are captured for recovery."),
            @Step(value = "Delete and recreate the namespace.", expected = "Namespace is deleted and recreated successfully."),
            @Step(value = "Recreate `PersistentVolumeClaims` and rebind `PersistentVolumes` resources.", expected = "The `PersistentVolumeClaim` resources are recreated and bound to the existing `PersistentVolumeClaim` resources."),
            @Step(value = "Recreate the Cluster Operator in the namespace.", expected = "The Cluster Operator is deployed and ready."),
            @Step(value = "Recreate all `KafkaTopic` resources.", expected = "All `KafkaTopic` resources are recreated successfully."),
            @Step(value = "Deploy the Kafka cluster with persistent storage.", expected = "Kafka cluster is deployed and becomes ready."),
            @Step(value = "Verify data recovery by producing and consuming messages.", expected = "Messages can be consumed, confirming data persisted through the recovery process.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testTopicAvailable() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        prepareEnvironmentForRecovery(testStorage);

        // Get list of topics and list of PVC needed for recovery
        List<KafkaTopic> kafkaTopicList = CrdClients.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).list().getItems();
        List<PersistentVolumeClaim> persistentVolumeClaimList = KubeResourceManager.get().kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace(testStorage.getNamespaceName());

        recreatePvcsAndRebindPvs(testStorage.getNamespaceName(), persistentVolumeClaimList);
        recreateClusterOperator(testStorage.getNamespaceName());

        // Recreate all KafkaTopic resources
        for (KafkaTopic kafkaTopic : kafkaTopicList) {
            kafkaTopic.getMetadata().setResourceVersion(null);
            CrdClients.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).resource(kafkaTopic).create();
        }

        KubeResourceManager.get().createResourceWithWait(
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

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                    .endPersistentClaimStorage()
                .endKafka()
            .endSpec()
            .build());

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @TestDoc(
        description = @Desc("This test verifies Kafka cluster recovery when `KafkaTopic` resources are not available after namespace deletion. The test deploys Kafka without the Topic Operator first to preserve existing topics, then enables Topic Operator after cluster becomes stable."),
        steps = {
            @Step(value = "Prepare test environment with a Kafka cluster and test data.", expected = "The Kafka cluster is deployed with persistent storage, and the topic contains test data."),
            @Step(value = "Store the cluster ID and list of `PersistentVolumeClaim` resources.", expected = "The Cluster ID and `PersistentVolumeClaim` list are captured for recovery."),
            @Step(value = "List the current topics in Kafka cluster.", expected = "The topic list is logged for verification."),
            @Step(value = "Delete and recreate the namespace.", expected = "The namespace is deleted and recreated successfully."),
            @Step(value = "Recreate `PersistentVolumeClaims` and rebind `PersistentVolumes` resources.", expected = "The `PersistentVolumeClaim` resources are recreated and bound to the existing `PersistentVolume` resources."),
            @Step(value = "Recreate the Cluster Operator in the namespace.", expected = "The Cluster Operator is deployed and ready."),
            @Step(value = "Deploy Kafka without the Topic Operator using the pause annotation.", expected = "The Kafka cluster is created without Topic Operator to prevent topic deletion."),
            @Step(value = "Patch the Kafka status with original cluster ID.", expected = "The Cluster ID is restored to match the original cluster."),
            @Step(value = "Unpause Kafka reconciliation.", expected = "The Kafka cluster becomes ready and operational."),
            @Step(value = "Enable the Topic Operator by updating the `Kafka` resource.", expected = "The Topic Operator is deployed and starts managing topics."),
            @Step(value = "Verify data recovery by producing and consuming messages.", expected = "Messages can be consumed, confirming that topics and data were preserved.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testTopicNotAvailable() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        prepareEnvironmentForRecovery(testStorage);

        // Get list of topics and list of PVC needed for recovery
        List<PersistentVolumeClaim> persistentVolumeClaimList = PersistentVolumeClaimUtils.listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("List of PVCs inside Namespace: {}", testStorage.getNamespaceName());
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            PersistentVolume pv = KubeResourceManager.get().kubeClient().getClient().persistentVolumes().withName(pvc.getSpec().getVolumeName()).get();
            LOGGER.info("Claim: {} has bounded Volume: {}", pvc.getMetadata().getName(), pv.getMetadata().getName());
        }

        // store cluster-id from Kafka CR
        final String oldClusterId = CrdClients.kafkaClient()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName())
            .get()
            .getStatus()
            .getClusterId();

        final String kafkaPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector()).get(0).getMetadata().getName();

        LOGGER.info("Currently present Topics inside Kafka: {}/{} are: {}", testStorage.getNamespaceName(), kafkaPodName,
            KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), kafkaPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName())));

        LOGGER.info("Deleting namespace and recreating for recovery");
        deleteAndRecreateNamespace(testStorage.getNamespaceName());

        LOGGER.info("Recreating PVCs and rebinding PVs for recovery");
        recreatePvcsAndRebindPvs(testStorage.getNamespaceName(), persistentVolumeClaimList);

        LOGGER.info("Recreating Cluster Operator");
        recreateClusterOperator(testStorage.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaComponents.getBrokerPoolName(testStorage.getClusterName()), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaComponents.getControllerPoolName(testStorage.getClusterName()), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endSpec()
                .build()
        );

        LOGGER.info("Recreating Kafka cluster without Topic Operator");
        KubeResourceManager.get().createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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
        CrdClients.kafkaClient()
            .inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName())
            .subresource("status")
            .patch(PatchContext.of(PatchType.JSON_MERGE), String.format("{\"status\": {\"clusterId\": \"%s\"}}", oldClusterId));

        //  Unpause the Kafka resource reconciliation
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            kafka -> {
                Map<String, String> annotations = kafka.getMetadata().getAnnotations();
                annotations.put(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false");
                kafka.getMetadata().setAnnotations(annotations);
            });

        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Currently present Topics inside Kafka: {}/{} are: {}", testStorage.getNamespaceName(), kafkaPodName,
            KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), kafkaPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName())));

        LOGGER.info("Adding Topic Operator to existing Kafka");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().setEntityOperator(new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .withNewUserOperator()
                .endUserOperator().build());
        });

        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), testStorage.getEoDeploymentName(), 1);

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
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

        KubeResourceManager.get().createResourceWithWait(
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

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("##################################################");
        LOGGER.info("Environment for recovery was successfully created");
        LOGGER.info("##################################################");
    }


    private void recreatePvcsAndRebindPvs(String namespaceName, List<PersistentVolumeClaim> persistentVolumeClaimList) {
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            pvc.getMetadata().setResourceVersion(null);
            pvc.getMetadata().setNamespace(namespaceName);

            KubeResourceManager.get().createResourceWithWait(pvc);

            PersistentVolume pv = KubeResourceManager.get().kubeClient().getClient().persistentVolumes().withName(pvc.getSpec().getVolumeName()).get();
            pv.getSpec().setClaimRef(null);
            KubeResourceManager.get().updateResource(pv);

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
        Namespace namespaceToBeDeleted = KubeResourceManager.get().kubeClient().getClient().namespaces().withName(namespace).get();
        KubeResourceManager.get().deleteResourceWithWait(namespaceToBeDeleted);

        // Recreate namespace
        NamespaceUtils.createNamespaceAndPrepare(namespace);
    }

    @BeforeAll
    void createStorageClass() {
        // Delete specific StorageClass if present from previous
        KubeResourceManager.get().kubeClient().getClient().storage().v1().storageClasses().withName(storageClassName).delete();

        final String storageClassKubernetesIo = "storageclass.kubernetes.io/is-default-class";
        // Get default StorageClass and change reclaim policy
        StorageClass defaultStorageClass =  KubeResourceManager.get().kubeClient().getClient().storage().v1().storageClasses().list().getItems().stream().filter(sg -> {
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

        KubeResourceManager.get().createResourceWithWait(retainStorageClass);
    }

    @AfterAll
    void teardown() {
        KubeResourceManager.get().kubeClient().getClient().persistentVolumes().list().getItems().stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains("kafka"))
            .forEach(pv -> KubeResourceManager.get().kubeClient().getClient().persistentVolumes().resource(pv).delete());
    }
}
