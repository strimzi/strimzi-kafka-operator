/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.generateRandomNameOfKafka;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying Cluster Operator's ability to recover Kafka cluster components from various deletion and failure scenarios."),
    beforeTestSteps = {
        @Step(value = "Deploy shared Kafka cluster and KafkaBridge.", expected = "Kafka cluster with 3 replicas and KafkaBridge is deployed.")
    }
)
class RecoveryST extends AbstractST {

    static String sharedClusterName;
    private static final int KAFKA_REPLICAS = 3;

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @TestDoc(
        description = @Desc("Test verifies that Cluster Operator can recover and recreate a deleted StrimziPodSet resource for Kafka brokers."),
        steps = {
            @Step(value = "Capture StrimziPodSet UID for Kafka brokers.", expected = "StrimziPodSet UID is recorded."),
            @Step(value = "Scale down Cluster Operator to 0 replicas.", expected = "Cluster Operator is stopped."),
            @Step(value = "Delete Kafka broker StrimziPodSet.", expected = "StrimziPodSet and associated pods are deleted."),
            @Step(value = "Scale up Cluster Operator to 1 replica.", expected = "Cluster Operator is running."),
            @Step(value = "Wait for StrimziPodSet recovery.", expected = "New StrimziPodSet is created with different UID and all pods are ready.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRecoveryFromKafkaStrimziPodSetDeletion() {
        // kafka cluster already deployed
        String kafkaName = KafkaComponents.getBrokerPodSetName(sharedClusterName);
        String kafkaUid = StrimziPodSetUtils.getStrimziPodSetUID(Environment.TEST_SUITE_NAMESPACE, kafkaName);

        KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).withTimeoutInMillis(600_000L).scale(0);
        StrimziPodSetUtils.deleteStrimziPodSet(Environment.TEST_SUITE_NAMESPACE, kafkaName);

        PodUtils.waitForPodsWithPrefixDeletion(Environment.TEST_SUITE_NAMESPACE, kafkaName);
        KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).withTimeoutInMillis(600_000L).scale(1);

        LOGGER.info("Waiting for recovery {}", kafkaName);
        StrimziPodSetUtils.waitForStrimziPodSetRecovery(Environment.TEST_SUITE_NAMESPACE, kafkaName, kafkaUid);
        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(Environment.TEST_SUITE_NAMESPACE, sharedClusterName, kafkaName, KAFKA_REPLICAS);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @TestDoc(
        description = @Desc("Test verifies that Cluster Operator can recover and recreate a deleted Kafka bootstrap Service."),
        steps = {
            @Step(value = "Capture Kafka bootstrap Service UID.", expected = "Service UID is recorded."),
            @Step(value = "Delete Kafka bootstrap Service.", expected = "Service is deleted."),
            @Step(value = "Wait for Service recovery.", expected = "New Service is created with different UID."),
            @Step(value = "Verify cluster stability by producing and consuming messages.", expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRecoveryFromKafkaServiceDeletion() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);

        String kafkaServiceName = KafkaResources.bootstrapServiceName(sharedClusterName);
        String kafkaServiceUid = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(kafkaServiceName).get().getMetadata().getUid();

        KubeResourceManager.get().kubeClient().getClient().services().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(kafkaServiceName).delete();

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        ServiceUtils.waitForServiceRecovery(Environment.TEST_SUITE_NAMESPACE, kafkaServiceName, kafkaServiceUid);
        verifyStabilityBySendingAndReceivingMessages(testStorage);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @TestDoc(
        description = @Desc("Test verifies that Cluster Operator can recover and recreate a deleted Kafka brokers headless Service."),
        steps = {
            @Step(value = "Capture Kafka brokers headless Service UID.", expected = "Service UID is recorded."),
            @Step(value = "Delete Kafka brokers headless Service.", expected = "Service is deleted."),
            @Step(value = "Wait for Service recovery.", expected = "New Service is created with different UID."),
            @Step(value = "Verify cluster stability by producing and consuming messages.", expected = "Messages are successfully produced and consumed.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRecoveryFromKafkaHeadlessServiceDeletion() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);

        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(sharedClusterName);
        String kafkaHeadlessServiceUid = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(kafkaHeadlessServiceName).get().getMetadata().getUid();

        KubeResourceManager.get().kubeClient().getClient().services().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(kafkaHeadlessServiceName).delete();

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(Environment.TEST_SUITE_NAMESPACE, kafkaHeadlessServiceName, kafkaHeadlessServiceUid);

        verifyStabilityBySendingAndReceivingMessages(testStorage);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @TestDoc(
        description = @Desc("Test verifies that Kafka cluster can recover from impossible resource requests by correcting the resource configuration."),
        steps = {
            @Step(value = "Update broker node pool with impossibly large memory request (465458732Gi).", expected = "Node pool is updated."),
            @Step(value = "Wait for Kafka pods to enter Pending state.", expected = "Kafka pods are in Pending state due to unsatisfiable resource requests."),
            @Step(value = "Verify pods remain stable in Pending state.", expected = "Pods stay in Pending state for stability period."),
            @Step(value = "Update broker node pool with reasonable memory request (512Mi).", expected = "Node pool is updated with schedulable resources."),
            @Step(value = "Wait for pods to become ready.", expected = "All Kafka pods are scheduled and running."),
            @Step(value = "Wait for Kafka cluster to be ready.", expected = "Kafka cluster reaches Ready state.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRecoveryFromImpossibleMemoryRequest() {
        final String kafkaSsName = KafkaComponents.getPodSetName(sharedClusterName, KafkaComponents.getBrokerPoolName(sharedClusterName));
        final LabelSelector brokerSelector = LabelSelectors.kafkaLabelSelector(sharedClusterName, kafkaSsName);
        final Map<String, Quantity> requests = new HashMap<>(1);

        requests.put("memory", new Quantity("465458732Gi"));
        final ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withRequests(requests)
            .build();

        KafkaNodePoolUtils.replace(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(sharedClusterName), knp -> knp.getSpec().setResources(resourceReq));

        PodUtils.waitForPendingPod(Environment.TEST_SUITE_NAMESPACE, kafkaSsName);
        PodUtils.verifyThatPendingPodsAreStable(Environment.TEST_SUITE_NAMESPACE, kafkaSsName);

        requests.put("memory", new Quantity("512Mi"));
        resourceReq.setRequests(requests);

        KafkaNodePoolUtils.replace(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(sharedClusterName), knp -> knp.getSpec().setResources(resourceReq));

        RollingUpdateUtils.waitForComponentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, brokerSelector, KAFKA_REPLICAS);
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, sharedClusterName);
    }

    private void verifyStabilityBySendingAndReceivingMessages(TestStorage testStorage) {
        KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(sharedClusterName));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test verifies that Kafka cluster can recover from multiple pod deletions by recreating the deleted pods."),
        steps = {
            @Step(value = "Delete majority of Kafka broker pods (all but one).", expected = "Most broker pods are deleted."),
            @Step(value = "Wait for all StrimziPodSets and pods to be ready.", expected = "Deleted pods are recreated and all pods are running."),
            @Step(value = "Wait for Kafka cluster to be ready.", expected = "Kafka cluster reaches Ready state.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRecoveryFromKafkaPodDeletion() {
        final String kafkaSPsName = KafkaComponents.getPodSetName(sharedClusterName, KafkaComponents.getBrokerPoolName(sharedClusterName));

        final LabelSelector brokerSelector = LabelSelectors.allKafkaPodsLabelSelector(sharedClusterName);

        LOGGER.info("Deleting most of the Kafka broker pods");
        List<Pod> kafkaPodList = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, brokerSelector);
        kafkaPodList.subList(0, kafkaPodList.size() - 1).forEach(pod -> KubeResourceManager.get().deleteResourceWithoutWait(pod));

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(Environment.TEST_SUITE_NAMESPACE, sharedClusterName, kafkaSPsName, KAFKA_REPLICAS);
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, sharedClusterName);
    }

    @BeforeEach
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
                .build()
            )
            .install();

        cluster.setNamespace(Environment.TEST_SUITE_NAMESPACE);

        sharedClusterName = generateRandomNameOfKafka("recovery-cluster");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(sharedClusterName), sharedClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getControllerPoolName(sharedClusterName), sharedClusterName, 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, sharedClusterName, KAFKA_REPLICAS).build());
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, sharedClusterName, KafkaResources.plainBootstrapAddress(sharedClusterName), 1).build());
    }

    @AfterEach
    void cleanup() {
        // In order to properly delete all resources, we need to delete KafkaNodePools first (as the correct deletion is KafkaNodePools -> Kafka)
        // This will ensure everything will be deleted properly
        List<KafkaNodePool> nodePools = CrdClients.kafkaNodePoolClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).list().getItems();
        KubeResourceManager.get().deleteResourceAsyncWait(nodePools.toArray(new KafkaNodePool[0]));
    }
}
