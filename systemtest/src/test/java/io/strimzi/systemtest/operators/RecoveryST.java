/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @Test
    void testRecoveryFromEntityOperatorDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", CLUSTER_NAME);
        String entityOperatorDeploymentName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        String entityOperatorDeploymentUid = kubeClient().getDeploymentUid(entityOperatorDeploymentName);
        kubeClient().deleteDeployment(entityOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(entityOperatorDeploymentName, entityOperatorDeploymentUid);
        DeploymentUtils.waitForDeploymentAndPodsReady(entityOperatorDeploymentName, 1);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    @Tag(ACCEPTANCE)
    void testRecoveryFromKafkaStatefulSetDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", CLUSTER_NAME);
        String kafkaStatefulSetName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String kafkaStatefulSetUid = kubeClient().getStatefulSetUid(kafkaStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(0, true);
        kubeClient().deleteStatefulSet(kafkaStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(1, true);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        StatefulSetUtils.waitForStatefulSetRecovery(kafkaStatefulSetName, kafkaStatefulSetUid);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaStatefulSetName, 3);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    @Tag(ACCEPTANCE)
    void testRecoveryFromZookeeperStatefulSetDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", CLUSTER_NAME);
        String zookeeperStatefulSetName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        String zookeeperStatefulSetUid = kubeClient().getStatefulSetUid(zookeeperStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(0, true);
        kubeClient().deleteStatefulSet(zookeeperStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(1, true);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        StatefulSetUtils.waitForStatefulSetRecovery(zookeeperStatefulSetName, zookeeperStatefulSetUid);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(zookeeperStatefulSetName, 1);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromKafkaServiceDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);
        String kafkaServiceName = KafkaResources.bootstrapServiceName(CLUSTER_NAME);
        String kafkaServiceUid = kubeClient().getServiceUid(kafkaServiceName);
        kubeClient().deleteService(kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaServiceName, kafkaServiceUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromZookeeperServiceDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);
        String zookeeperServiceName = KafkaResources.zookeeperServiceName(CLUSTER_NAME);
        String zookeeperServiceUid = kubeClient().getServiceUid(zookeeperServiceName);
        kubeClient().deleteService(zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        ServiceUtils.waitForServiceRecovery(zookeeperServiceName, zookeeperServiceUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromKafkaHeadlessServiceDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);
        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(CLUSTER_NAME);
        String kafkaHeadlessServiceUid = kubeClient().getServiceUid(kafkaHeadlessServiceName);
        kubeClient().deleteService(kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaHeadlessServiceName, kafkaHeadlessServiceUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromZookeeperHeadlessServiceDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);
        String zookeeperHeadlessServiceName = KafkaResources.zookeeperHeadlessServiceName(CLUSTER_NAME);
        String zookeeperHeadlessServiceUid = kubeClient().getServiceUid(zookeeperHeadlessServiceName);
        kubeClient().deleteService(zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(zookeeperHeadlessServiceName, zookeeperHeadlessServiceUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromKafkaMetricsConfigDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", CLUSTER_NAME);
        String kafkaMetricsConfigName = KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME);
        String kafkaMetricsConfigUid = kubeClient().getConfigMapUid(kafkaMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaMetricsConfigName, kafkaMetricsConfigUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromZookeeperMetricsConfigDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = KafkaResources.zookeeperMetricsAndLogConfigMapName(CLUSTER_NAME);
        String zookeeperMetricsConfigUid = kubeClient().getConfigMapUid(zookeeperMetricsConfigName);
        kubeClient().deleteConfigMap(zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(zookeeperMetricsConfigName, zookeeperMetricsConfigUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeDeploymentDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeDeployment with cluster {}", CLUSTER_NAME);
        // kafka cluster already deployed
        String kafkaBridgeDeploymentName = KafkaBridgeResources.deploymentName(CLUSTER_NAME);
        String kafkaBridgeDeploymentUid = kubeClient().getDeploymentUid(kafkaBridgeDeploymentName);
        kubeClient().deleteDeployment(kafkaBridgeDeploymentName);

        LOGGER.info("Waiting for deployment {} recovery", kafkaBridgeDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(kafkaBridgeDeploymentName, kafkaBridgeDeploymentUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeServiceDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeService with cluster {}", CLUSTER_NAME);
        String kafkaBridgeServiceName = KafkaBridgeResources.serviceName(CLUSTER_NAME);
        String kafkaBridgeServiceUid = kubeClient().namespace(NAMESPACE).getServiceUid(kafkaBridgeServiceName);
        kubeClient().deleteService(kafkaBridgeServiceName);

        LOGGER.info("Waiting for service {} recovery", kafkaBridgeServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaBridgeServiceName, kafkaBridgeServiceUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeMetricsConfigDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeMetricsConfig with cluster {}", CLUSTER_NAME);
        String kafkaBridgeMetricsConfigName = KafkaBridgeResources.metricsAndLogConfigMapName(CLUSTER_NAME);
        String kafkaBridgeMetricsConfigUid = kubeClient().getConfigMapUid(kafkaBridgeMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaBridgeMetricsConfigName);

        LOGGER.info("Waiting for metric config {} re-creation", kafkaBridgeMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaBridgeMetricsConfigName, kafkaBridgeMetricsConfigUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();
    }
}
