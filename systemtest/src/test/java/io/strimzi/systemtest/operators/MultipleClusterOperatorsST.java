/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.setupCOMetricsCollectorInNamespace;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
public class MultipleClusterOperatorsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleClusterOperatorsST.class);

    public static final String DEFAULT_NAMESPACE = "multiple-co-cluster-test";
    public static final String FIRST_NAMESPACE = "first-co-namespace";
    public static final String SECOND_NAMESPACE = "second-co-namespace";

    public static final String FIRST_CO_NAME = "first-" + TestConstants.STRIMZI_DEPLOYMENT_NAME;
    public static final String SECOND_CO_NAME = "second-" + TestConstants.STRIMZI_DEPLOYMENT_NAME;

    public static final EnvVar FIRST_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "app.kubernetes.io/operator=" + FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "app.kubernetes.io/operator=" + SECOND_CO_NAME, null);

    public static final EnvVar FIRST_CO_LEASE_NAME_ENV = new EnvVar("STRIMZI_LEADER_ELECTION_LEASE_NAME", FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_LEASE_NAME_ENV = new EnvVar("STRIMZI_LEADER_ELECTION_LEASE_NAME", SECOND_CO_NAME, null);

    public static final Map<String, String> FIRST_CO_SELECTOR = Collections.singletonMap("app.kubernetes.io/operator", FIRST_CO_NAME);
    public static final Map<String, String> SECOND_CO_SELECTOR = Collections.singletonMap("app.kubernetes.io/operator", SECOND_CO_NAME);

    /**
     * @description This test case checks how two Cluster Operators operates operands deployed in namespace watched by both of these operators, and transition
     * of operand from one Cluster Operator to another.
     *
     * @steps
     *  1. - Deploy two Cluster Operators - first to the first namespace and second to the second namespace - both watching all namespaces.
     *     - Cluster Operators are successfully deployed.
     *  2. - Set up scrapers and metric collectors for both of Cluster Operators.
     *     - Mentioned resources are successfully set up.
     *  3. - Create and Set namespace 'multiple-co-cluster-test' as default, because all resources from now on will be deployed here.
     *     - Namespace is created and set as default.
     *  4. - Deploy Kafka operand in default namespace without label 'app.kubernetes.io/operator'.
     *     - Kafka is not deployed.
     *  5. - Verify state of metric 'strimzi_resource' for Kafka, which holds number of operated Kafka operands.
     *     - Metric 'strimzi_resource' exposed by Cluster Operators is not present indicating no Kafka resource is being operated.
     *  6. - Modify Kafka custom resource, by changing its label 'app.kubernetes.io/operator' to point to the first Cluster Operator
     *     - Kafka is deployed and operated by the first Cluster Operator.
     *  7. - Deploy Kafka Connect (with label 'app.kubernetes.io/operator' pointing the first Cluster Operator) and  Kafka Connector
     *     - Both operands are successfully deployed, and managed by the first Cluster Operator.
     *  8. - Produce messages into the Kafka Topic and consume with Sink Connector.
     *     - Messages are produced and later handled correctly by the Connector.
     *  9. - Management of Kafka is switched to the second Cluster Operator by modifying value of label 'app.kubernetes.io/operator'.
     *     - Management is modified, later confirmed by the expected change in the value of metric 'strimzi_resource' for kind Kafka on both Cluster Operators.
     *  10. - Verify 'strimzi_resource' of each Cluster Operator for all deployed operands.
     *      - Metric 'strimzi_resource' indicates expected number of operated resources for both Cluster Operators.
     *
     * @usecase
     *  - cluster-operator-metrics
     *  - cluster-operator-watcher
     *  - connect
     *  - kafka
     *  - labels
     *  - metrics
     */
    @IsolatedTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testMultipleCOsInDifferentNamespaces() {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), DEFAULT_NAMESPACE);

        String firstCOScraperName = FIRST_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        String secondCOScraperName = SECOND_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;

        LOGGER.info("Deploying Cluster Operators: {}, {} in respective namespaces: {}, {}", FIRST_CO_NAME, SECOND_CO_NAME, FIRST_NAMESPACE, SECOND_NAMESPACE);
        deployCOInNamespace(FIRST_CO_NAME, FIRST_NAMESPACE, Collections.singletonList(FIRST_CO_SELECTOR_ENV), true);
        deployCOInNamespace(SECOND_CO_NAME, SECOND_NAMESPACE, Collections.singletonList(SECOND_CO_SELECTOR_ENV), true);

        LOGGER.info("Deploying scraper Pods: {}, {} for later metrics retrieval", firstCOScraperName, secondCOScraperName);
        resourceManager.createResourceWithWait(
            ScraperTemplates.scraperPod(FIRST_NAMESPACE, firstCOScraperName).build(),
            ScraperTemplates.scraperPod(SECOND_NAMESPACE, secondCOScraperName).build()
        );

        LOGGER.info("Setting up metric collectors targeting Cluster Operators: {}, {}", FIRST_CO_NAME, SECOND_CO_NAME);
        String firstCOScraper = FIRST_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        MetricsCollector firstCoMetricsCollector = setupCOMetricsCollectorInNamespace(FIRST_CO_NAME, FIRST_NAMESPACE, firstCOScraper);
        String secondCOScraper = SECOND_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        MetricsCollector secondCoMetricsCollector = setupCOMetricsCollectorInNamespace(SECOND_CO_NAME, SECOND_NAMESPACE, secondCOScraper);

        // allowing NetworkPolicies for all scraper Pods to all CO Pods
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(FIRST_NAMESPACE);
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(SECOND_NAMESPACE);

        LOGGER.info("Deploying Namespace: {} to host all additional operands", testStorage.getNamespaceName());
        NamespaceManager.getInstance().createNamespaceAndPrepare(testStorage.getNamespaceName());

        LOGGER.info("Set cluster namespace to {}, as all operands will be from now on deployd here", testStorage.getNamespaceName());
        cluster.setNamespace(testStorage.getNamespaceName());

        LOGGER.info("Deploying Kafka: {}/{} without CR selector", testStorage.getNamespaceName(), testStorage.getClusterName());
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3).build());

        // checking that no pods with prefix 'clusterName' will be created in some time
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), testStorage.getClusterName(), 0);

        // verify that metric signalizing managing of kafka is not present in either of cluster operators
        MetricsUtils.assertCoMetricResourcesNullOrZero(firstCoMetricsCollector, Kafka.RESOURCE_KIND, testStorage.getNamespaceName());
        MetricsUtils.assertCoMetricResourcesNullOrZero(secondCoMetricsCollector, Kafka.RESOURCE_KIND, testStorage.getNamespaceName());

        LOGGER.info("Adding {} selector of {} into Kafka: {} CR", FIRST_CO_SELECTOR, FIRST_CO_NAME, testStorage.getClusterName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getMetadata().setLabels(FIRST_CO_SELECTOR), testStorage.getNamespaceName());
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
                .editOrNewMetadata()
                    .addToLabels(FIRST_CO_SELECTOR)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build());

        String kafkaConnectPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaConnector with file sink and CR selector - {} - different than selector in Kafka", SECOND_CO_SELECTOR);
        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", testStorage.getTopicName())
            .endSpec()
            .build());

        final KafkaClients basicClients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(basicClients.producerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        LOGGER.info("Verifying that all operands in Namespace: {} are managed by Cluster Operator: {}", testStorage.getNamespaceName(), FIRST_CO_NAME);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(firstCoMetricsCollector, Kafka.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(firstCoMetricsCollector, KafkaConnect.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(firstCoMetricsCollector, KafkaConnector.RESOURCE_KIND, 1);

        LOGGER.info("Switch management of Kafka Cluster: {}/{} operand from CO: {} to CO: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), FIRST_CO_NAME, SECOND_CO_NAME);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getMetadata().getLabels().replace("app.kubernetes.io/operator", SECOND_CO_NAME);
        }, testStorage.getNamespaceName());

        LOGGER.info("Verifying that number of managed Kafka resources in increased in CO: {} and decreased om CO: {}", SECOND_CO_NAME, FIRST_CO_NAME);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(secondCoMetricsCollector, Kafka.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesLowerThanOrEqualTo(firstCoMetricsCollector, Kafka.RESOURCE_KIND, 0);
    }

    /**
     * @description This test case checks how two Cluster Operators deployed in the same namespace operates operands including KafkaRebalance and transition
     * of operand from one Cluster Operator to another.
     *
     * @steps
     *  1. - Deploy 2 Cluster Operators in the same namespace, with additional env variable 'STRIMZI_LEADER_ELECTION_LEASE_NAME'.
     *     - Cluster Operators are successfully deployed.
     *  2. - Set up scrapers and metric collectors for first Cluster Operators.
     *  3. - Deploy Kafka Cluster with 3 Kafka replicas and label 'app.kubernetes.io/operator' pointing to the first Cluster Operator.
     *  4. - Change Kafka's label selector 'app.kubernetes.io/operator' to point to not existing Cluster Operator.
     *     - Kafka Cluster is no longer controlled by any Cluster Operator.
     *  5. - Modify Kafka custom resource, by increasing number of replicas from 3 to 4.
     *     - Kafka is not scaled to 4 replicas.
     *  6. - Deploy Kafka Rebalance without 'app.kubernetes.io/operator' label.
     *     - For a stable period of time, Kafka Rebalance is ignored as well.
     *  7. - Change Kafka's label selector 'app.kubernetes.io/operator' to point to the second Cluster Operator.
     *     - Second Cluster Operator now operates Kafka Cluster and increases its replica count to 4.
     *  8. - Cruise Control Pod is rolled as there is increase in Kafka replica count.
     *     - Rebalance finally takes place.
     *  9. - Verify that Operators operate expected operands.
     *     - Operators operate expected operands.
     *
     * @usecase
     *  - cluster-operator-metrics
     *  - cluster-operator-watcher
     *  - kafka
     *  - labels
     *  - metrics
     *  - rebalance
     */
    @IsolatedTest
    @Tag(CRUISE_CONTROL)
    @KRaftNotSupported("The scaling of the Kafka Pods is not working properly at the moment")
    void testKafkaCCAndRebalanceWithMultipleCOs() {
        assumeFalse(Environment.isNamespaceRbacScope());
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), DEFAULT_NAMESPACE);

        int scaleTo = 4;

        LOGGER.info("Deploying 2 Cluster Operators: {}, {} in the same namespace: {}", FIRST_CO_NAME, SECOND_CO_NAME, testStorage.getNamespaceName());
        deployCOInNamespace(FIRST_CO_NAME, testStorage.getNamespaceName(), List.of(FIRST_CO_SELECTOR_ENV, FIRST_CO_LEASE_NAME_ENV), false);
        deployCOInNamespace(SECOND_CO_NAME, testStorage.getNamespaceName(), List.of(SECOND_CO_SELECTOR_ENV, SECOND_CO_LEASE_NAME_ENV), false);

        String secondCOScraperName = testStorage.getNamespaceName() + "-" + TestConstants.SCRAPER_NAME;

        LOGGER.info("Deploying scraper Pod: {}, for later metrics retrieval", secondCOScraperName);
        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(testStorage.getNamespaceName(), secondCOScraperName).build());

        LOGGER.info("Setting up metric collectors targeting Cluster Operator: {}", SECOND_CO_NAME);
        String coScraperName = testStorage.getNamespaceName() + "-" + TestConstants.SCRAPER_NAME;
        MetricsCollector secondCoMetricsCollector = setupCOMetricsCollectorInNamespace(SECOND_CO_NAME, testStorage.getNamespaceName(), coScraperName);
        // allowing NetworkPolicies for all scraper Pods to all CO Pods
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(testStorage.getNamespaceName());

        LOGGER.info("Deploying Kafka with cruise control and with {} selector of {}", FIRST_CO_NAME, FIRST_CO_SELECTOR);
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
            .editOrNewMetadata()
                .addToLabels(FIRST_CO_SELECTOR)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());

        final Map<String, String> kafkaCCSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Removing CR selector from Kafka and increasing number of replicas to 4, new Pod should not appear");
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp -> {
                Map<String, String> labels = knp.getMetadata().getLabels();
                labels.put("app.kubernetes.io/operator", "random-operator-value");

                knp.getMetadata().setLabels(labels);
                knp.getSpec().setReplicas(scaleTo);
            }, testStorage.getNamespaceName());
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getMetadata().getLabels().clear();
            kafka.getSpec().getKafka().setReplicas(scaleTo);
        }, testStorage.getNamespaceName());

        // because KafkaRebalance is pointing to Kafka with CC cluster, we need to create KR before adding the label back
        // to test if KR will be ignored
        LOGGER.info("Creating KafkaRebalance when CC doesn't have label for CO, the KR should be ignored");
        resourceManager.createResourceWithoutWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal",
                    "NetworkInboundCapacityGoal", "MinTopicLeadersPerBrokerGoal",
                    "NetworkOutboundCapacityGoal", "ReplicaCapacityGoal")
                .withSkipHardGoalCheck(true)
                // skip sanity check: because of removal 'RackAwareGoal'
            .endSpec()
            .build());

        KafkaUtils.waitForClusterStability(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking if KafkaRebalance is still ignored, after the cluster stability wait");

        // because KR is ignored, it shouldn't contain any status
        assertNull(KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus());

        LOGGER.info("Adding {} selector of {} to Kafka", SECOND_CO_SELECTOR, SECOND_CO_NAME);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(),
                knp -> knp.getMetadata().getLabels().putAll(SECOND_CO_SELECTOR), testStorage.getNamespaceName());
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getMetadata().setLabels(SECOND_CO_SELECTOR), testStorage.getNamespaceName());

        LOGGER.info("Waiting for Kafka to scales Pods to {}", scaleTo);
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaleTo);
        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).size(), is(scaleTo));

        LOGGER.info("Waiting for CC Pod to roll, because there is change in kafka replication factor");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, kafkaCCSnapshot);

        KafkaUtils.waitForClusterStability(testStorage.getNamespaceName(), testStorage.getClusterName());

        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Verifying that operands are operated by expected Cluster Operator {}", FIRST_CO_NAME);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(secondCoMetricsCollector, KafkaRebalance.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(secondCoMetricsCollector, Kafka.RESOURCE_KIND, 1);
    }

    void deployCOInNamespace(String coName, String coNamespace, List<EnvVar> extraEnvs, boolean multipleNamespaces) {
        String namespace = multipleNamespaces ? TestConstants.WATCH_ALL_NAMESPACES : coNamespace;

        if (multipleNamespaces) {
            // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
            List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(coNamespace, coName);
            clusterRoleBindingList.forEach(
                clusterRoleBinding -> ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding));
        }

        LOGGER.info("Creating: {} in Namespace: {}", coName, coNamespace);

        clusterOperator = clusterOperator.defaultInstallation()
            .withNamespace(coNamespace)
            .withClusterOperatorName(coName)
            .withWatchingNamespaces(namespace)
            .withExtraLabels(Collections.singletonMap("app.kubernetes.io/operator", coName))
            .withExtraEnvVars(extraEnvs)
            .createInstallation()
            .runBundleInstallation();
    }

    @BeforeAll
    void setup() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
    }
}
