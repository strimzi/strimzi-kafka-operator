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
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.rollingupdate.KafkaRollerST;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.generateRandomNameOfKafka;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static String clusterName;
    static String kafkaClientsName;

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @Test
    void testRecoveryFromEntityOperatorDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", clusterName);
        String entityOperatorDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);
        String entityOperatorDeploymentUid = kubeClient().getDeploymentUid(entityOperatorDeploymentName);
        kubeClient().deleteDeployment(entityOperatorDeploymentName);
        PodUtils.waitForPodsWithPrefixDeletion(entityOperatorDeploymentName);
        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(entityOperatorDeploymentName, entityOperatorDeploymentUid);
        DeploymentUtils.waitForDeploymentAndPodsReady(entityOperatorDeploymentName, 1);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromKafkaStatefulSetDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", clusterName);
        String kafkaStatefulSetName = KafkaResources.kafkaStatefulSetName(clusterName);
        String kafkaStatefulSetUid = kubeClient().getStatefulSetUid(kafkaStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(0, true);
        kubeClient().deleteStatefulSet(kafkaStatefulSetName);
        PodUtils.waitForPodsWithPrefixDeletion(kafkaStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(1, true);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        StatefulSetUtils.waitForStatefulSetRecovery(kafkaStatefulSetName, kafkaStatefulSetUid);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaStatefulSetName, 3);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromZookeeperStatefulSetDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", clusterName);
        String zookeeperStatefulSetName = KafkaResources.zookeeperStatefulSetName(clusterName);
        String zookeeperStatefulSetUid = kubeClient().getStatefulSetUid(zookeeperStatefulSetName);
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).scale(0, true);
        kubeClient().deleteStatefulSet(zookeeperStatefulSetName);
        PodUtils.waitForPodsWithPrefixDeletion(zookeeperStatefulSetName);
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
        LOGGER.info("Running deleteKafkaService with cluster {}", clusterName);
        String kafkaServiceName = KafkaResources.bootstrapServiceName(clusterName);
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
        LOGGER.info("Running deleteKafkaService with cluster {}", clusterName);
        String zookeeperServiceName = KafkaResources.zookeeperServiceName(clusterName);
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
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", clusterName);
        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(clusterName);
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
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", clusterName);
        String zookeeperHeadlessServiceName = KafkaResources.zookeeperHeadlessServiceName(clusterName);
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
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", clusterName);
        String kafkaMetricsConfigName = KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName);
        String kafkaMetricsConfigUid = kubeClient().getConfigMapUid(kafkaMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaMetricsConfigName, kafkaMetricsConfigUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testRecoveryFromZookeeperMetricsConfigDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", clusterName);
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName);
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
        LOGGER.info("Running deleteKafkaBridgeDeployment with cluster {}", clusterName);
        // kafka cluster already deployed
        String kafkaBridgeDeploymentName = KafkaBridgeResources.deploymentName(clusterName);
        String kafkaBridgeDeploymentUid = kubeClient().getDeploymentUid(kafkaBridgeDeploymentName);
        kubeClient().deleteDeployment(kafkaBridgeDeploymentName);
        PodUtils.waitForPodsWithPrefixDeletion(kafkaBridgeDeploymentName);
        LOGGER.info("Waiting for deployment {} recovery", kafkaBridgeDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(kafkaBridgeDeploymentName, kafkaBridgeDeploymentUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeServiceDeletion() {
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeService with cluster {}", clusterName);
        String kafkaBridgeServiceName = KafkaBridgeResources.serviceName(clusterName);
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
        LOGGER.info("Running deleteKafkaBridgeMetricsConfig with cluster {}", clusterName);
        String kafkaBridgeMetricsConfigName = KafkaBridgeResources.metricsAndLogConfigMapName(clusterName);
        String kafkaBridgeMetricsConfigUid = kubeClient().getConfigMapUid(kafkaBridgeMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaBridgeMetricsConfigName);

        LOGGER.info("Waiting for metric config {} re-creation", kafkaBridgeMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaBridgeMetricsConfigName, kafkaBridgeMetricsConfigUid);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
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
     * @see KafkaRollerST#testKafkaPodPending()
     */
    @Test
    void testRecoveryFromImpossibleMemoryRequest() {
        String clusterName = "my-cluster";
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(clusterName);

        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("memory", new Quantity("465458732Gi"));

        ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withRequests(requests)
            .build();

        Kafka kafka = KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withResources(resourceReq)
                .endKafka()
            .endSpec()
            .build());

        PodUtils.waitForPendingPod(kafkaSsName);
        PodUtils.verifyThatPendingPodsAreStable(kafkaSsName);

        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        requests.put("memory", new Quantity("512Mi"));
        resourceReq.setRequests(requests);

        KafkaResource.replaceKafkaResource(clusterName, k -> k.getSpec().getKafka().setResources(resourceReq));

        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaSsName, 3);
        KafkaUtils.waitForKafkaReady(clusterName);

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        KafkaResource.kafkaClient().inNamespace(NAMESPACE).delete(kafka);
    }

    @BeforeEach
    void setup() throws Exception {
        ResourceManager.setMethodResources();
        installClusterOperator(NAMESPACE);
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
        clusterName = generateRandomNameOfKafka("recovery-cluster");
        kafkaClientsName = Constants.KAFKA_CLIENTS + "-" + clusterName;

        KafkaResource.kafkaEphemeral(clusterName, 3, 1).done();
        KafkaClientsResource.deployKafkaClients(false, kafkaClientsName).done();
        KafkaBridgeResource.kafkaBridge(clusterName, KafkaResources.plainBootstrapAddress(clusterName), 1).done();
    }
}
