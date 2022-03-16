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
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.StrimziPodSetTest;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.rollingupdate.KafkaRollerIsolatedST;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.generateRandomNameOfKafka;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
@IsolatedSuite
class RecoveryIsolatedST extends AbstractST {

    static String sharedClusterName;
    private static final int KAFKA_REPLICAS = 3;
    private static final int ZOOKEEPER_REPLICAS = 3;

    private static final Logger LOGGER = LogManager.getLogger(RecoveryIsolatedST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromEntityOperatorDeletion() {
        // kafka cluster already deployed
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", sharedClusterName);

        String entityOperatorDeploymentName = KafkaResources.entityOperatorDeploymentName(sharedClusterName);
        String entityOperatorDeploymentUid = kubeClient().getDeploymentUid(entityOperatorDeploymentName);

        kubeClient().deleteDeployment(entityOperatorDeploymentName);
        PodUtils.waitForPodsWithPrefixDeletion(entityOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);

        DeploymentUtils.waitForDeploymentRecovery(entityOperatorDeploymentName, entityOperatorDeploymentUid);
        DeploymentUtils.waitForDeploymentAndPodsReady(entityOperatorDeploymentName, 1);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaStatefulSetDeletion() {
        // kafka cluster already deployed
        String kafkaName = KafkaResources.kafkaStatefulSetName(sharedClusterName);
        String kafkaUid = StUtils.getStrimziPodSetOrStatefulSetUID(kafkaName);

        kubeClient().getClient().apps().deployments().inNamespace(INFRA_NAMESPACE).withName(Constants.STRIMZI_DEPLOYMENT_NAME).scale(0, true);
        StUtils.deleteStrimziPodSetOrStatefulSet(kafkaName);

        PodUtils.waitForPodsWithPrefixDeletion(kafkaName);
        kubeClient().getClient().apps().deployments().inNamespace(INFRA_NAMESPACE).withName(Constants.STRIMZI_DEPLOYMENT_NAME).scale(1, true);

        LOGGER.info("Waiting for recovery {}", kafkaName);
        StUtils.waitForStrimziPodSetOrStatefulSetRecovery(kafkaName, kafkaUid);
        StUtils.waitForStrimziPodSetOrStatefulSetAndPodsReady(kafkaName, KAFKA_REPLICAS);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperStatefulSetDeletion() {
        // kafka cluster already deployed
        String zookeeperName = KafkaResources.zookeeperStatefulSetName(sharedClusterName);
        String zookeeperUid = StUtils.getStrimziPodSetOrStatefulSetUID(zookeeperName);

        kubeClient().getClient().apps().deployments().inNamespace(INFRA_NAMESPACE).withName(Constants.STRIMZI_DEPLOYMENT_NAME).scale(0, true);
        StUtils.deleteStrimziPodSetOrStatefulSet(zookeeperName);

        PodUtils.waitForPodsWithPrefixDeletion(zookeeperName);
        kubeClient().getClient().apps().deployments().inNamespace(INFRA_NAMESPACE).withName(Constants.STRIMZI_DEPLOYMENT_NAME).scale(1, true);

        LOGGER.info("Waiting for recovery {}", zookeeperName);
        StUtils.waitForStrimziPodSetOrStatefulSetRecovery(zookeeperName, zookeeperUid);
        StUtils.waitForStrimziPodSetOrStatefulSetAndPodsReady(zookeeperName, ZOOKEEPER_REPLICAS);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaServiceDeletion() {
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);

        String kafkaServiceName = KafkaResources.bootstrapServiceName(sharedClusterName);
        String kafkaServiceUid = kubeClient().getServiceUid(kafkaServiceName);

        kubeClient().deleteService(kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaServiceName, kafkaServiceUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperServiceDeletion() {
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);

        String zookeeperServiceName = KafkaResources.zookeeperServiceName(sharedClusterName);
        String zookeeperServiceUid = kubeClient().getServiceUid(zookeeperServiceName);

        kubeClient().deleteService(zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        ServiceUtils.waitForServiceRecovery(zookeeperServiceName, zookeeperServiceUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaHeadlessServiceDeletion() {
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);

        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(sharedClusterName);
        String kafkaHeadlessServiceUid = kubeClient().getServiceUid(kafkaHeadlessServiceName);

        kubeClient().deleteService(kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaHeadlessServiceName, kafkaHeadlessServiceUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperHeadlessServiceDeletion() {
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);

        String zookeeperHeadlessServiceName = KafkaResources.zookeeperHeadlessServiceName(sharedClusterName);
        String zookeeperHeadlessServiceUid = kubeClient().getServiceUid(zookeeperHeadlessServiceName);

        kubeClient().deleteService(zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(zookeeperHeadlessServiceName, zookeeperHeadlessServiceUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaMetricsConfigDeletion() {
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", sharedClusterName);

        String kafkaMetricsConfigName = KafkaResources.kafkaMetricsAndLogConfigMapName(sharedClusterName);
        String kafkaMetricsConfigUid = kubeClient().getConfigMapUid(kafkaMetricsConfigName);

        kubeClient().deleteConfigMap(kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaMetricsConfigName, kafkaMetricsConfigUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromZookeeperMetricsConfigDeletion() {
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", sharedClusterName);

        // kafka cluster already deployed
        String zookeeperMetricsConfigName = KafkaResources.zookeeperMetricsAndLogConfigMapName(sharedClusterName);
        String zookeeperMetricsConfigUid = kubeClient().getConfigMapUid(zookeeperMetricsConfigName);

        kubeClient().deleteConfigMap(zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(zookeeperMetricsConfigName, zookeeperMetricsConfigUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeDeploymentDeletion() {
        LOGGER.info("Running deleteKafkaBridgeDeployment with cluster {}", sharedClusterName);

        // kafka cluster already deployed
        String kafkaBridgeDeploymentName = KafkaBridgeResources.deploymentName(sharedClusterName);
        String kafkaBridgeDeploymentUid = kubeClient().getDeploymentUid(kafkaBridgeDeploymentName);

        kubeClient().deleteDeployment(kafkaBridgeDeploymentName);
        PodUtils.waitForPodsWithPrefixDeletion(kafkaBridgeDeploymentName);

        LOGGER.info("Waiting for deployment {} recovery", kafkaBridgeDeploymentName);
        DeploymentUtils.waitForDeploymentRecovery(kafkaBridgeDeploymentName, kafkaBridgeDeploymentUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeServiceDeletion() {
        LOGGER.info("Running deleteKafkaBridgeService with cluster {}", sharedClusterName);
        String kafkaBridgeServiceName = KafkaBridgeResources.serviceName(sharedClusterName);
        String kafkaBridgeServiceUid = kubeClient().namespace(INFRA_NAMESPACE).getServiceUid(kafkaBridgeServiceName);
        kubeClient().deleteService(kafkaBridgeServiceName);

        LOGGER.info("Waiting for service {} recovery", kafkaBridgeServiceName);
        ServiceUtils.waitForServiceRecovery(kafkaBridgeServiceName, kafkaBridgeServiceUid);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeMetricsConfigDeletion() {
        LOGGER.info("Running deleteKafkaBridgeMetricsConfig with cluster {}", sharedClusterName);
        String kafkaBridgeMetricsConfigName = KafkaBridgeResources.metricsAndLogConfigMapName(sharedClusterName);
        String kafkaBridgeMetricsConfigUid = kubeClient().getConfigMapUid(kafkaBridgeMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaBridgeMetricsConfigName);

        LOGGER.info("Waiting for metric config {} re-creation", kafkaBridgeMetricsConfigName);
        ConfigMapUtils.waitForConfigMapRecovery(kafkaBridgeMetricsConfigName, kafkaBridgeMetricsConfigUid);
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
     * @see {@link KafkaRollerIsolatedST#testKafkaPodPending(ExtensionContext)}
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromImpossibleMemoryRequest() {
        final String kafkaSsName = KafkaResources.kafkaStatefulSetName(sharedClusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(sharedClusterName, KafkaResources.kafkaStatefulSetName(sharedClusterName));
        final Map<String, Quantity> requests = new HashMap<>(1);

        requests.put("memory", new Quantity("465458732Gi"));
        final ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withRequests(requests)
            .build();

        KafkaResource.replaceKafkaResource(sharedClusterName, k -> k.getSpec().getKafka().setResources(resourceReq));

        PodUtils.waitForPendingPod(kafkaSsName);
        PodUtils.verifyThatPendingPodsAreStable(kafkaSsName);

        requests.put("memory", new Quantity("512Mi"));
        resourceReq.setRequests(requests);

        KafkaResource.replaceKafkaResource(sharedClusterName, k -> k.getSpec().getKafka().setResources(resourceReq));

        RollingUpdateUtils.waitForComponentAndPodsReady(INFRA_NAMESPACE, kafkaSelector, KAFKA_REPLICAS);
        KafkaUtils.waitForKafkaReady(sharedClusterName);
    }

    @IsolatedTest
    @StrimziPodSetTest
    void testRecoveryFromKafkaAndZookeeperPodDeletion() {
        final String kafkaName = KafkaResources.kafkaStatefulSetName(sharedClusterName);
        final String zkName = KafkaResources.zookeeperStatefulSetName(sharedClusterName);

        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(sharedClusterName, kafkaName);
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(sharedClusterName, zkName);

        LOGGER.info("Deleting most of the Kafka and ZK pods");
        List<Pod> kafkaPodList = kubeClient().listPods(kafkaSelector);
        List<Pod> zkPodList = kubeClient().listPods(zkSelector);

        kafkaPodList.subList(0, kafkaPodList.size() - 1).forEach(pod -> kubeClient().deletePod(pod));
        zkPodList.subList(0, zkPodList.size() - 1).forEach(pod -> kubeClient().deletePod(pod));

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(kafkaName, KAFKA_REPLICAS);
        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(zkName, ZOOKEEPER_REPLICAS);
        KafkaUtils.waitForKafkaReady(sharedClusterName);
    }

    @BeforeEach
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withReconciliationInterval(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        sharedClusterName = generateRandomNameOfKafka("recovery-cluster");
        String kafkaClientsName = Constants.KAFKA_CLIENTS + "-" + sharedClusterName;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(sharedClusterName, KAFKA_REPLICAS, ZOOKEEPER_REPLICAS).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(sharedClusterName, KafkaResources.plainBootstrapAddress(sharedClusterName), 1).build());
    }
}
