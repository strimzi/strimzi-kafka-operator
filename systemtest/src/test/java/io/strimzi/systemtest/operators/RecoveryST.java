/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.generateRandomNameOfKafka;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static String sharedClusterName;

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromEntityOperatorDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", sharedClusterName);
        String entityOperatorDeploymentName = KafkaResources.entityOperatorDeploymentName(sharedClusterName);
        String entityOperatorDeploymentUid = kubeClient().getDeploymentUid(entityOperatorDeploymentName);
        kubeClient().deleteDeployment(entityOperatorDeploymentName);
        PodUtils.waitForPodsWithPrefixDeletion(entityOperatorDeploymentName);
        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(entityOperatorDeploymentName, entityOperatorDeploymentUid);
        DeploymentUtils.waitForDeploymentAndPodsReady(entityOperatorDeploymentName, 1);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaStatefulSetDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        String kafkaStatefulSetName = KafkaResources.kafkaStatefulSetName(sharedClusterName);
        String kafkaStatefulSetUid = kubeClient().getStatefulSetUid(kafkaStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(0, true);
        kubeClient().deleteStatefulSet(kafkaStatefulSetName);
        PodUtils.waitForPodsWithPrefixDeletion(kafkaStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(1, true);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        StatefulSetUtils.waitForStatefulSetRecovery(kafkaStatefulSetName, kafkaStatefulSetUid);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaStatefulSetName, 3);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperStatefulSetDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", sharedClusterName);
        String zookeeperStatefulSetName = KafkaResources.zookeeperStatefulSetName(sharedClusterName);
        String zookeeperStatefulSetUid = kubeClient().getStatefulSetUid(zookeeperStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(0, true);
        kubeClient().deleteStatefulSet(zookeeperStatefulSetName);
        PodUtils.waitForPodsWithPrefixDeletion(zookeeperStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(1, true);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        StatefulSetUtils.waitForStatefulSetRecovery(zookeeperStatefulSetName, zookeeperStatefulSetUid);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(zookeeperStatefulSetName, 1);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaServiceDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);
        String kafkaServiceName = KafkaResources.bootstrapServiceName(sharedClusterName);
        String kafkaServiceUid = kubeClient().getServiceUid(kafkaServiceName);
        kubeClient().deleteService(kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaServiceName, kafkaServiceUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperServiceDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);
        String zookeeperServiceName = KafkaResources.zookeeperServiceName(sharedClusterName);
        String zookeeperServiceUid = kubeClient().getServiceUid(zookeeperServiceName);
        kubeClient().deleteService(zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        ServiceUtils.waitForServiceRecovery(zookeeperServiceName, zookeeperServiceUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaHeadlessServiceDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);
        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(sharedClusterName);
        String kafkaHeadlessServiceUid = kubeClient().getServiceUid(kafkaHeadlessServiceName);
        kubeClient().deleteService(kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaHeadlessServiceName, kafkaHeadlessServiceUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperHeadlessServiceDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);
        String zookeeperHeadlessServiceName = KafkaResources.zookeeperHeadlessServiceName(sharedClusterName);
        String zookeeperHeadlessServiceUid = kubeClient().getServiceUid(zookeeperHeadlessServiceName);
        kubeClient().deleteService(zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(zookeeperHeadlessServiceName, zookeeperHeadlessServiceUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaMetricsConfigDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", sharedClusterName);
        String kafkaMetricsConfigName = KafkaResources.kafkaMetricsAndLogConfigMapName(sharedClusterName);
        String kafkaMetricsConfigUid = kubeClient().getConfigMapUid(kafkaMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaMetricsConfigName, kafkaMetricsConfigUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperMetricsConfigDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", sharedClusterName);
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = KafkaResources.zookeeperMetricsAndLogConfigMapName(sharedClusterName);
        String zookeeperMetricsConfigUid = kubeClient().getConfigMapUid(zookeeperMetricsConfigName);
        kubeClient().deleteConfigMap(zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(zookeeperMetricsConfigName, zookeeperMetricsConfigUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeDeploymentDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        LOGGER.info("Running deleteKafkaBridgeDeployment with cluster {}", sharedClusterName);
        // kafka cluster already deployed
        String kafkaBridgeDeploymentName = KafkaBridgeResources.deploymentName(sharedClusterName);
        String kafkaBridgeDeploymentUid = kubeClient().getDeploymentUid(kafkaBridgeDeploymentName);
        kubeClient().deleteDeployment(kafkaBridgeDeploymentName);
        PodUtils.waitForPodsWithPrefixDeletion(kafkaBridgeDeploymentName);
        LOGGER.info("Waiting for deployment {} recovery", kafkaBridgeDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(kafkaBridgeDeploymentName, kafkaBridgeDeploymentUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeServiceDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        LOGGER.info("Running deleteKafkaBridgeService with cluster {}", sharedClusterName);
        String kafkaBridgeServiceName = KafkaBridgeResources.serviceName(sharedClusterName);
        String kafkaBridgeServiceUid = kubeClient().namespace(NAMESPACE).getServiceUid(kafkaBridgeServiceName);
        kubeClient().deleteService(kafkaBridgeServiceName);

        LOGGER.info("Waiting for service {} recovery", kafkaBridgeServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaBridgeServiceName, kafkaBridgeServiceUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeMetricsConfigDeletion(ExtensionContext extensionContext) {
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        LOGGER.info("Running deleteKafkaBridgeMetricsConfig with cluster {}", sharedClusterName);
        String kafkaBridgeMetricsConfigName = KafkaBridgeResources.metricsAndLogConfigMapName(sharedClusterName);
        String kafkaBridgeMetricsConfigUid = kubeClient().getConfigMapUid(kafkaBridgeMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaBridgeMetricsConfigName);

        LOGGER.info("Waiting for metric config {} re-creation", kafkaBridgeMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaBridgeMetricsConfigName, kafkaBridgeMetricsConfigUid);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
    }

    /**
     * The main difference between this test and KafkaRollerST#testKafkaPodPending()
     * is that in this test, we are deploying Kafka cluster with an impossible memory request,
     * but in the KafkaRollerST#testKafkaPodPending()
     * we first deploy Kafka cluster with a correct configuration, then change the configuration to an unschedulable one, waiting
     * for one Kafka pod to be in the `Pending` phase. In this test, all 3 Kafka pods are `Pending`. After we
     * check that Kafka pods are stable in `Pending` phase (for one minute), we change the memory request so that the pods are again schedulable
     * and wait until the Kafka cluster recovers and becomes `Ready`.
     *
     * @see {@link io.strimzi.systemtest.rollingupdate.KafkaRollerST#testKafkaPodPending(ExtensionContext)}
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromImpossibleMemoryRequest(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(clusterName);

        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("memory", new Quantity("465458732Gi"));

        ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withRequests(requests)
            .build();

        Kafka kafka = KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withResources(resourceReq)
                .endKafka()
            .endSpec()
            .build();

        resourceManager.createResource(extensionContext, false, kafka);

        PodUtils.waitForPendingPod(kafkaSsName);
        PodUtils.verifyThatPendingPodsAreStable(kafkaSsName);

        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        requests.put("memory", new Quantity("512Mi"));
        resourceReq.setRequests(requests);

        KafkaResource.replaceKafkaResource(clusterName, k -> k.getSpec().getKafka().setResources(resourceReq));

        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaSsName, 3);
        KafkaUtils.waitForKafkaReady(clusterName);

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        KafkaResource.kafkaClient().inNamespace(NAMESPACE).delete(kafka);
    }

    @BeforeEach
    void setup(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String clusterOperatorName = clusterName + "-" + Constants.STRIMZI_DEPLOYMENT_NAME;

        installClusterOperator(extensionContext, clusterOperatorName, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);

        sharedClusterName = generateRandomNameOfKafka("recovery-cluster");
        String kafkaClientsName = Constants.KAFKA_CLIENTS + "-" + sharedClusterName;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(sharedClusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(sharedClusterName, KafkaResources.plainBootstrapAddress(sharedClusterName), 1).build());
    }

    @BeforeAll
    void prepare(ExtensionContext extensionContext) {
        prepareEnvForOperator(extensionContext, NAMESPACE);
        if (Environment.isNamespaceRbacScope()) {
            // if roles only, only deploy the rolebindings
            applyRoleBindings(extensionContext, NAMESPACE, NAMESPACE);
        } else {
            applyBindings(extensionContext, NAMESPACE);
        }
    }
}
