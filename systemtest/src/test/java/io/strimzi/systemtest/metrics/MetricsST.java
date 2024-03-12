/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.METRICS;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.SANITY;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourceState;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourceStateNotExists;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResources;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertCoMetricResourcesNullOrZero;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricCountHigherThan;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResources;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValue;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueCount;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueHigherThan;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueNullOrZero;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.getExporterRunScript;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.getResourceMetricPattern;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * @description This test suite is designed for testing metrics exposed by operators and operands.
 *
 * @info the class should be executed without issues with all the following combinations
 *  - All install types
 *  - All feature gates
 *  - In parallel with restrictions based on test-case annotations
 *
 * @beforeAll
 *  1. - Create namespaces {@namespaceFirst} and {@namespaceSecond}
 *     - Namespaces {@namespaceFirst} and {@namespaceSecond} are created
 *  2. - Deploy Cluster Operator
 *     - Cluster Operator is deployed
 *  3. - Deploy Kafka {@kafkaClusterFirstName} with metrics and CruiseControl configured
 *     - Kafka @{kafkaClusterFirstName} is deployed
 *  4. - Deploy Kafka {@kafkaClusterSecondtName} with metrics configured
 *     - Kafka @{kafkaClusterFirstName} is deployed
 *  5. - Deploy scraper Pods in namespace {@namespaceFirst} and {@namespaceSecond} for collecting metrics from Strimzi pods
 *     - Scraper Pods are deployed
 *  6. - Create KafkaUsers and KafkaTopics
 *     - All KafkaUsers and KafkaTopics are Ready
 *  7. - Setup NetworkPolicies to grant access to Operator Pods and KafkaExporter
 *     - NetworkPolicies created
 *  8. - Create collectors for Cluster Operator, Kafka, KafkaExporter, and Zookeeper (Non-KRaft)
 *     - Metrics collected in collectors structs
 *
 * @afterAll
 *  1. - Common cleaning of all resources created by this test class
 *     - All resources deleted.
 *
 * @updated 2023-17-04
 */
@Tag(SANITY)
@Tag(ACCEPTANCE)
@Tag(REGRESSION)
@Tag(METRICS)
@Tag(CRUISE_CONTROL)
public class MetricsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);

    private final String namespaceFirst = "metrics-test-0";
    private final String namespaceSecond = "metrics-test-1";
    private final String kafkaClusterFirstName = "metrics-cluster-0";
    private final String kafkaClusterSecondName = "metrics-cluster-1";
    private final String mm2ClusterName = "mm2-cluster";
    private final String bridgeClusterName = "my-bridge";

    private String coScraperPodName;
    private String testSuiteScraperPodName;
    private String scraperPodName;
    private String secondNamespaceScraperPodName;

    private String bridgeTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
    private final String kafkaExporterTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

    private MetricsCollector kafkaCollector;
    private MetricsCollector zookeeperCollector;
    private MetricsCollector kafkaExporterCollector;
    private MetricsCollector clusterOperatorCollector;

    /**
     * @description This test case check several random metrics exposed by Kafka.
     *
     * @steps
     *  1. - Check if specific metric is available in collected metrics from Kafka Pods
     *     - Metric is available with expected value
     *
     * @usecase
     *  - metrics
     *  - kafka-metrics
     */
    @ParallelTest
    @Tag(ACCEPTANCE)
    void testKafkaMetrics() {
        assertMetricValueCount(kafkaCollector, "kafka_server_replicamanager_leadercount", 3);
        assertMetricCountHigherThan(kafkaCollector, "kafka_server_replicamanager_partitioncount", 2);
        assertMetricValue(kafkaCollector, "kafka_server_replicamanager_underreplicatedpartitions", 0);
    }

    /**
     * @description This test case check several random metrics exposed by Zookeeper.
     *
     * @steps
     *  1. - Check if specific metric is available in collected metrics from Zookeeper Pods
     *     - Metric is available with expected value
     *
     * @usecase
     *  - metrics
     *  - zookeeper-metrics
     */
    @ParallelTest
    @Tag(ACCEPTANCE)
    @KRaftNotSupported("ZooKeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperMetrics() {
        assertMetricValueNotNull(zookeeperCollector, "zookeeper_quorumsize");
        assertMetricCountHigherThan(zookeeperCollector, "zookeeper_numaliveconnections\\{.*\\}", 0L);
        assertMetricCountHigherThan(zookeeperCollector, "zookeeper_inmemorydatatree_watchcount\\{.*\\}", 0L);
    }

    /**
     * @description This test case check several random metrics exposed by Kafka Connect.
     *
     * @steps
     *  1. - Deploy KafkaConnect into {@namespaceFirst} with {@Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES} set to true
     *     - KafkaConnect is up and running
     *  2. - Create KafkaConnector for KafkaConnect from step 1
     *     - KafkaConnector is in Ready state.
     *  3. - Create metrics collector and collect metrics from KafkaConnect Pods
     *     - Metrics are collected
     *  4. - Check if specific metric is available in collected metrics from KafkaConnect Pods
     *     - Metric is available with expected value
     *  5. - Collect current metrics from Cluster Operator Pod
     *     - Cluster Operator metrics are collected
     *  6. - Check that CO metrics contain data about KafkaConnect and KafkaConnector in namespace {@namespaceFirst}
     *     - CO metrics contain expected data
     *  7. - Check that CO metrics don't contain data about KafkaConnect and KafkaConnector in namespace {@namespaceFirst}
     *     - CO metrics don't contain expected data
     *  8. - Check that CO metrics contain data about KafkaConnect state
     *     - CO metrics contain expected data
     *
     * @usecase
     *  - metrics
     *  - connect-metrics
     *  - cluster-operator-metrics
     */
    @ParallelTest
    @KindIPv6NotSupported("error checking push permissions -- make sure you entered the correct tag name, and that " +
            "you are authenticated correctly, and try again: checking push permission for " +
            "\"myregistry.local:5001/metrics-test-0/strimzi-sts-connect-build:1904341592\": creating push check " +
            "transport for myregistry.local:5001 failed: Get \"https://myregistry.local:5001/v2/\": dial tcp: lookup " +
            "myregistry.local on [fd00:10:96::a]:53: server misbehaving; Get \"http://myregistry.local:5001/v2/\": dial " +
            "tcp: lookup myregistry.local on [fd00:10:96::a]:53: server misbehaving")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectAndConnectorMetrics() {
        resourceManager.createResourceWithWait(
            KafkaConnectTemplates.kafkaConnectWithMetricsAndFileSinkPlugin(kafkaClusterFirstName, namespaceFirst, kafkaClusterFirstName, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build());
        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(kafkaClusterFirstName).build());

        MetricsCollector kafkaConnectCollector = kafkaCollector.toBuilder()
                .withComponentType(ComponentType.KafkaConnect)
                .build();

        kafkaConnectCollector.collectMetricsFromPods();

        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_request_total\\{clientid=\".*\",}", 0);
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_node_response_total\\{clientid=\".*\",.*}", 0);
        assertMetricValueHigherThan(kafkaConnectCollector, "kafka_connect_network_io_total\\{clientid=\".*\",.*}", 0);

        // Check CO metrics and look for KafkaConnect and KafkaConnector
        clusterOperatorCollector.collectMetricsFromPods();
        assertCoMetricResources(clusterOperatorCollector, KafkaConnect.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaConnect.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceState(clusterOperatorCollector, KafkaConnect.RESOURCE_KIND, kafkaClusterFirstName, namespaceFirst, 1, "none");

        assertCoMetricResources(clusterOperatorCollector, KafkaConnector.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaConnector.RESOURCE_KIND, namespaceSecond);

        assertMetricValueHigherThan(clusterOperatorCollector, getResourceMetricPattern(StrimziPodSet.RESOURCE_KIND, namespaceFirst), 1);
        assertMetricValueHigherThan(clusterOperatorCollector, getResourceMetricPattern(StrimziPodSet.RESOURCE_KIND, namespaceSecond), 0);
    }

    /**
     * @description This test case check several metrics exposed by KafkaExporter.
     *
     * @steps
     *  1. - Create Kafka producer and consumer and exchange some messages
     *     - Clients successfully exchange the messages
     *  2. - Check if metric kafka_topic_partitions is available in collected metrics from KafkaExporter Pods
     *     - Metric is available with expected value
     *  3. - Check if metric kafka_broker_info is available in collected metrics from KafkaExporter pods for each Kafka Broker pod
     *     - Metric is available with expected value
     *
     * @usecase
     *  - metrics
     *  - kafka-exporter-metrics
     */
    @IsolatedTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaExporterMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String kafkaStrimziPodSetName = StrimziPodSetResource.getBrokerComponentName(kafkaClusterFirstName);
        final LabelSelector brokerPodsSelector = KafkaResource.getLabelSelector(kafkaClusterFirstName, kafkaStrimziPodSetName);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(kafkaExporterTopicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterFirstName))
            .withNamespaceName(namespaceFirst)
            .withMessageCount(5000)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), namespaceFirst, testStorage.getMessageCount(), false);

        assertMetricValueNotNull(kafkaExporterCollector, "kafka_consumergroup_current_offset\\{.*\\}");

        if (!Environment.isKRaftModeEnabled()) {
            Pattern pattern = Pattern.compile("kafka_topic_partitions\\{topic=\"" + kafkaExporterTopicName + "\"} ([\\d])");
            ArrayList<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s doesn't contain correct value", pattern), values.stream().mapToDouble(i -> i).sum(), is(7.0));
        }

        kubeClient().listPods(namespaceFirst, brokerPodsSelector).forEach(pod -> {
            String address = pod.getMetadata().getName() + "." + kafkaClusterFirstName + "-kafka-brokers." + namespaceFirst + ".svc";
            Pattern pattern = Pattern.compile("kafka_broker_info\\{address=\"" + address + ".*\",.*} ([\\d])");
            ArrayList<Double> values = kafkaExporterCollector.waitForSpecificMetricAndCollect(pattern);
            assertThat(String.format("metric %s is not null", pattern), values, notNullValue());
        });
    }

    /**
     * @description This test case check several metrics exposed by KafkaExporter with different from default configuration.
     * Rolling update is performed during the test case to change KafkaExporter configuration.
     *
     * @steps
     *  1. - Get KafkaExporter run.sh script and check it has configured proper values
     *     - Script has proper values set
     *  2. - Check that KafkaExporter metrics contains info about consumer_offset topic
     *     - Metrics contains proper data
     *  3. - Change configuration of KafkaExporter in Kafka CR to match 'my-group.*' group regex and {@topicName} as topic name regex, than wait for KafkaExporter rolling update.
     *     - Rolling update finished
     *  4. - Get KafkaExporter run.sh script and check it has configured proper values
     *     - Script has proper values set
     *  5. - Check that KafkaExporter metrics don't contain info about consumer_offset topic
     *     - Metrics contains proper data (consumer_offset is not in the metrics)
     *  6. - Revert all changes in KafkaExporter configuration and wait for Rolling Update
     *     - Rolling update finished
     *
     * @usecase
     *  - metrics
     *  - kafka-exporter-metrics
     */
    @ParallelTest
    void testKafkaExporterDifferentSetting() throws InterruptedException, ExecutionException, IOException {
        String consumerOffsetsTopicName = "__consumer_offsets";
        LabelSelector exporterSelector = kubeClient().getDeploymentSelectors(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName));
        String runScriptContent = getExporterRunScript(kubeClient().listPods(namespaceFirst, exporterSelector).get(0).getMetadata().getName(), namespaceFirst);
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\".*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\".*\""));
        // Check that metrics contains info about consumer_offsets
        assertMetricValueNotNull(kafkaExporterCollector, "kafka_topic_partitions\\{topic=\"" + consumerOffsetsTopicName + "\"}");

        Map<String, String> kafkaExporterSnapshot = DeploymentUtils.depSnapshot(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(kafkaClusterFirstName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex("my-group.*");
            k.getSpec().getKafkaExporter().setTopicRegex(topicName);
        }, namespaceFirst);

        kafkaExporterSnapshot = DeploymentUtils.waitTillDepHasRolled(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(kubeClient().listPods(namespaceFirst, exporterSelector).get(0).getMetadata().getName(), namespaceFirst);
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + topicName + "\""));

        // Check that metrics don't contain info about consumer_offsets
        assertMetricValueNullOrZero(kafkaExporterCollector, "kafka_topic_partitions\\{topic=\"" + consumerOffsetsTopicName + "\"}");

        LOGGER.info("Changing Topic and group regexes back to default");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(kafkaClusterFirstName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex(".*");
            k.getSpec().getKafkaExporter().setTopicRegex(".*");
        }, namespaceFirst);

        DeploymentUtils.waitTillDepHasRolled(namespaceFirst, KafkaExporterResources.componentName(kafkaClusterFirstName), 1, kafkaExporterSnapshot);
    }

    /**
     * @description This test case check several random metrics exposed by CLuster Operator.
     *
     * @steps
     *  1. - Check that specific metric for Kafka reconciliation are available in metrics from Cluster Operator pod
     *     - Metric is available with expected value
     *  2. - Check that collected metrics contain data about Kafka resource
     *     - Metric is available with expected value
     *  3. - Check that collected metrics don't contain data about KafkaMirrorMaker and KafkaRebalance resource
     *     - Metric is not exposed
     *
     * @usecase
     *  - metrics
     *  - cluster-operator-metrics
     */
    @ParallelTest
    void testClusterOperatorMetrics() {
        // Expected PodSet counts per component
        int podSetCount = 2;

        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_periodical_total", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_count", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_sum", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_max", Kafka.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_successful_total", Kafka.RESOURCE_KIND);

        assertCoMetricResources(clusterOperatorCollector, Kafka.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResources(clusterOperatorCollector, Kafka.RESOURCE_KIND, namespaceSecond, 1);
        assertCoMetricResourceState(clusterOperatorCollector, Kafka.RESOURCE_KIND, kafkaClusterFirstName, namespaceFirst, 1, "none");
        assertCoMetricResourceState(clusterOperatorCollector, Kafka.RESOURCE_KIND, kafkaClusterSecondName, namespaceSecond, 1, "none");

        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaMirrorMaker.RESOURCE_KIND, namespaceFirst);
        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaMirrorMaker.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceStateNotExists(clusterOperatorCollector, KafkaMirrorMaker.RESOURCE_KIND, namespaceFirst, kafkaClusterFirstName);

        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaRebalance.RESOURCE_KIND, namespaceFirst);
        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaRebalance.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceStateNotExists(clusterOperatorCollector, KafkaRebalance.RESOURCE_KIND, namespaceFirst, kafkaClusterFirstName);

        // check StrimziPodSet metrics in CO
        assertMetricCountHigherThan(clusterOperatorCollector, getResourceMetricPattern(StrimziPodSet.RESOURCE_KIND, namespaceFirst), 0);
        assertCoMetricResources(clusterOperatorCollector, StrimziPodSet.RESOURCE_KIND, namespaceSecond, podSetCount);

        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_bucket", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_count", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_sum", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_duration_seconds_max", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_already_enqueued_total", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_successful_total", StrimziPodSet.RESOURCE_KIND);
        assertCoMetricResourceNotNull(clusterOperatorCollector, "strimzi_reconciliations_total", StrimziPodSet.RESOURCE_KIND);
    }

    /**
     * @description This test case check several metrics exposed by User Operator.
     *
     * @steps
     *  1. - Collect metrics from User Operator pod
     *     - Metrics are collected
     *  2. - Check that specific metrics about KafkaUser are available in collected metrics
     *     - Metric is available with expected value
     *
     * @usecase
     *  - metrics
     *  - user-operator-metrics
     */
    @ParallelTest
    void testUserOperatorMetrics() {
        MetricsCollector userOperatorCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.UserOperator)
            .build();

        userOperatorCollector.collectMetricsFromPods();

        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_successful_total", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_count", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_sum", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_duration_seconds_max", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_periodical_total", KafkaUser.RESOURCE_KIND);
        assertMetricResourceNotNull(userOperatorCollector, "strimzi_reconciliations_total", KafkaUser.RESOURCE_KIND);

        assertMetricResources(userOperatorCollector, KafkaUser.RESOURCE_KIND, namespaceFirst, 2);
    }

    /**
     * @description This test case check several metrics exposed by KafkaMirrorMaker2.
     *
     * @steps
     *  1. - Deploy KafkaMirrorMaker2 into {@namespaceFirst}
     *     - KafkaMirrorMaker2 is in Ready state
     *  2. - Collect metrics from KafkaMirrorMaker2 pod
     *     - Metrics are collected
     *  3. - Check if specific metric is available in collected metrics from KafkaMirrorMaker2 pods
     *     - Metric is available with expected value
     *  4. - Collect current metrics from Cluster Operator pod
     *     - Cluster Operator metrics are collected
     *  5. - Check that CO metrics contain data about KafkaMirrorMaker2 in namespace {@namespaceFirst}
     *     - CO metrics contain expected data
     *
     * @usecase
     *  - metrics
     *  - mirrormaker2-metrics
     *  - cluster-operator-metrics
     */
    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testMirrorMaker2Metrics() {
        resourceManager.createResourceWithWait(
                KafkaMirrorMaker2Templates.kafkaMirrorMaker2WithMetrics(namespaceFirst, mm2ClusterName, kafkaClusterFirstName, kafkaClusterSecondName, 1, namespaceSecond, namespaceFirst)
                    .editMetadata()
                        .withNamespace(namespaceFirst)
                    .endMetadata()
                    .build());

        MetricsCollector kmm2Collector = kafkaCollector.toBuilder()
            .withComponentName(mm2ClusterName)
            .withComponentType(ComponentType.KafkaMirrorMaker2)
            .build();

        assertMetricValue(kmm2Collector, "kafka_connect_worker_connector_count", 3);
        assertMetricValue(kmm2Collector, "kafka_connect_worker_task_count", 1);

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods();
        assertCoMetricResources(clusterOperatorCollector, KafkaMirrorMaker2.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaMirrorMaker2.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceState(clusterOperatorCollector, KafkaMirrorMaker2.RESOURCE_KIND, mm2ClusterName, namespaceFirst, 1, "none");
        assertMetricValueHigherThan(clusterOperatorCollector, getResourceMetricPattern(StrimziPodSet.RESOURCE_KIND, namespaceFirst), 1);
    }

    /**
     * @description This test case check several metrics exposed by KafkaBridge.
     *
     * @steps
     *  1. - Deploy KafkaBridge into {@namespaceFirst}
     *     - KafkaMirrorMaker2 is in Ready state
     *  2. - Attach producer and consumer clients to KafkaBridge
     *     - Clients and up and running
     *  3. - Collect metrics from KafkaBridge pod
     *     - Metrics are collected
     *  4. - Check that specific metric is available in collected metrics from KafkaBridge pods
     *     - Metric is available with expected value
     *  5. - Collect current metrics from Cluster Operator pod
     *     - Cluster Operator metrics are collected
     *  6. - Check that CO metrics contain data about KafkaBridge in namespace {@namespaceFirst}
     *     - CO metrics contain expected data
     *
     * @usecase
     *  - metrics
     *  - kafka-bridge-metrics
     *  - cluster-operator-metrics
     */
    @ParallelTest
    @Tag(BRIDGE)
    void testKafkaBridgeMetrics() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
                KafkaBridgeTemplates.kafkaBridgeWithMetrics(bridgeClusterName, kafkaClusterFirstName, KafkaResources.plainBootstrapAddress(kafkaClusterFirstName), 1)
                    .editMetadata()
                        .withNamespace(namespaceFirst)
                    .endMetadata()
                    .build());

        // Allow connections from scraper to Bridge pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForBridgeScraper(namespaceFirst, scraperPodName, KafkaBridgeResources.componentName(bridgeClusterName));

        MetricsCollector bridgeCollector = kafkaCollector.toBuilder()
            .withComponentName(bridgeClusterName)
            .withComponentType(ComponentType.KafkaBridge)
            .build();

        // Attach consumer before producer
        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withNamespaceName(namespaceFirst)
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaBridgeResources.serviceName(bridgeClusterName))
            .withComponentName(KafkaBridgeResources.componentName(bridgeClusterName))
            .withTopicName(bridgeTopicName)
            .withMessageCount(testStorage.getMessageCount())
            .withPort(TestConstants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(200)
            .withPollInterval(200)
            .build();

        // we cannot wait for producer and consumer to complete to see all needed metrics - especially `strimzi_bridge_kafka_producer_count`
        resourceManager.createResourceWithWait(kafkaBridgeClientJob.producerStrimziBridge(), kafkaBridgeClientJob.consumerStrimziBridge());

        bridgeCollector.collectMetricsFromPods();
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_producer_count\\{.*,}");
        assertMetricValueNotNull(bridgeCollector, "strimzi_bridge_kafka_consumer_connection_count\\{.*,}");
        assertThat("bridge collected data don't contain strimzi_bridge_http_server", bridgeCollector.getCollectedData().values().toString().contains("strimzi_bridge_http_server"));

        // Check CO metrics and look for KafkaBridge
        clusterOperatorCollector.collectMetricsFromPods();
        assertCoMetricResources(clusterOperatorCollector, KafkaBridge.RESOURCE_KIND, namespaceFirst, 1);
        assertCoMetricResourcesNullOrZero(clusterOperatorCollector, KafkaBridge.RESOURCE_KIND, namespaceSecond);
        assertCoMetricResourceState(clusterOperatorCollector, KafkaBridge.RESOURCE_KIND, bridgeClusterName, namespaceFirst, 1, "none");
    }

    /**
     * @description This test case check several random metrics exposed by CruiseControl.
     *
     * @steps
     *  1. - Check if specific metric is available in collected metrics from CruiseControl pods
     *     - Metric is available with expected value
     *
     * @usecase
     *  - metrics
     *  - cruise-control-metrics
     */
    @ParallelTest
    void testCruiseControlMetrics() {
        String cruiseControlMetrics = CruiseControlUtils.callApi(namespaceFirst, CruiseControlUtils.HttpMethod.GET, CruiseControlUtils.Scheme.HTTP,
                CruiseControlUtils.CRUISE_CONTROL_METRICS_PORT, "/metrics", "", true).getResponseText();
        Matcher regex = Pattern.compile("^([^#].*)\\s+([^\\s]*)$", Pattern.MULTILINE).matcher(cruiseControlMetrics);

        LOGGER.info("Verifying that we have more than 0 groups");

        assertThat(regex.groupCount(), greaterThan(0));

        while (regex.find()) {

            String metricKey = regex.group(1);
            Object metricValue = regex.group(2);

            LOGGER.debug("{} -> {}", metricKey, metricValue);

            assertThat(metricKey, not(nullValue()));
            assertThat(metricValue, not(nullValue()));
        }
    }

    /**
     * @description This test case check that Cluster Operator propagate changes from metrics configuration done in kafka CR into corresponding config map.
     *
     * @steps
     *  1. - Create config map with external metrics configuration
     *     - Config map created
     *  2. - Set ConfigMap reference from step 1 into Kafka CR and wait for pod stabilization (CO shouldn't trigger rolling update)
     *     - Wait for Kafka pods stability (60 seconds without rolling update in the row)
     *  3. - Check that metrics config maps for each pod contains data from external metrics config map
     *     - All config maps contains proper values
     *  4. - Change config in external metrics config map
     *     - Config map changed
     *  5. - SWait for Kafka pods stabilization (CO shouldn't trigger rolling update)
     *     - Wait for Kafka pods stability (60 seconds without rolling update in the row)
     *  6. - Check that metrics config maps for each pod contains data from external metrics config map
     *     - All config maps contains proper values
     *
     * @usecase
     *  - metrics
     *  - kafka-metrics-rolling-update
     *  - kafka-metrics-external
     */
    @ParallelTest
    void testKafkaMetricsSettings() {
        String metricsConfigJson = "{\"lowercaseOutputName\":true}";
        String metricsConfigYaml = "lowercaseOutputName: true";

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(
                JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),
                true
        );

        ConfigMap externalMetricsCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(TestConstants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(namespaceSecond)
                .endMetadata()
                .build();

        kubeClient().createConfigMapInNamespace(namespaceSecond, externalMetricsCm);

        // spec.kafka.metrics -> spec.kafka.jmxExporterMetrics
        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("external-metrics-cm")
                .withKey(TestConstants.METRICS_CONFIG_YAML_NAME)
                .build();
        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(kafkaClusterSecondName, k -> {
            k.getSpec().getKafka().setMetricsConfig(jmxPrometheusExporterMetrics);
        }, namespaceSecond);

        PodUtils.verifyThatRunningPodsAreStable(namespaceSecond, kafkaClusterSecondName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(namespaceSecond, kafkaClusterSecondName)) {
            ConfigMap actualCm = kubeClient(namespaceSecond).getConfigMap(cmName);
            assertThat(actualCm.getData().get(TestConstants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson));
        }

        // update metrics
        ConfigMap externalMetricsUpdatedCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(TestConstants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml.replace("true", "false")))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(namespaceSecond)
                .endMetadata()
                .build();

        kubeClient().updateConfigMapInNamespace(namespaceSecond, externalMetricsUpdatedCm);
        PodUtils.verifyThatRunningPodsAreStable(namespaceSecond, kafkaClusterSecondName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(namespaceSecond, kafkaClusterSecondName)) {
            ConfigMap actualCm = kubeClient(namespaceSecond).getConfigMap(cmName);
            assertThat(actualCm.getData().get(TestConstants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson.replace("true", "false")));
        }
    }

    @BeforeAll
    void setupEnvironment() throws Exception {
        // Metrics tests are not designed to run with namespace RBAC scope.
        assumeFalse(Environment.isNamespaceRbacScope());
        NamespaceManager.getInstance().createNamespaces(Environment.TEST_SUITE_NAMESPACE, CollectorElement.createCollectorElement(this.getClass().getName()), Arrays.asList(namespaceFirst, namespaceSecond));

        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        final String coScraperName = TestConstants.CO_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        final String testSuiteScraperName = Environment.TEST_SUITE_NAMESPACE + "-" + TestConstants.SCRAPER_NAME;
        final String scraperName = namespaceFirst + "-" + TestConstants.SCRAPER_NAME;
        final String secondScraperName = namespaceSecond + "-" + TestConstants.SCRAPER_NAME;

        cluster.setNamespace(namespaceFirst);

        // create resources without wait to deploy them simultaneously
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(namespaceFirst, KafkaNodePoolResource.getBrokerPoolName(kafkaClusterFirstName), kafkaClusterFirstName, 3).build(),
                KafkaNodePoolTemplates.controllerPool(namespaceFirst, KafkaNodePoolResource.getControllerPoolName(kafkaClusterFirstName), kafkaClusterFirstName, 3).build(),
                KafkaNodePoolTemplates.brokerPool(namespaceSecond, KafkaNodePoolResource.getBrokerPoolName(kafkaClusterSecondName), kafkaClusterSecondName, 1).build(),
                KafkaNodePoolTemplates.controllerPool(namespaceSecond, KafkaNodePoolResource.getControllerPoolName(kafkaClusterSecondName), kafkaClusterSecondName, 1).build()
            )
        );
        resourceManager.createResourceWithoutWait(
            KafkaTemplates.kafkaMetricsConfigMap(namespaceFirst, kafkaClusterFirstName),
            KafkaTemplates.cruiseControlMetricsConfigMap(namespaceFirst, kafkaClusterFirstName),
            // Kafka with CruiseControl and metrics
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(namespaceFirst, kafkaClusterFirstName, 3, 3)
                .editOrNewSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(30)
                        .endTopicOperator()
                        .editUserOperator()
                            .withReconciliationIntervalSeconds(30)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build(),
            KafkaTemplates.kafkaMetricsConfigMap(namespaceSecond, kafkaClusterSecondName),
            KafkaTemplates.kafkaWithMetrics(namespaceSecond, kafkaClusterSecondName, 1, 1).build(),
            ScraperTemplates.scraperPod(TestConstants.CO_NAMESPACE, coScraperName).build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, testSuiteScraperName).build(),
            ScraperTemplates.scraperPod(namespaceFirst, scraperName).build(),
            ScraperTemplates.scraperPod(namespaceSecond, secondScraperName).build()
        );

        // sync resources
        resourceManager.synchronizeResources();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(kafkaClusterFirstName, topicName, 7, 2, namespaceFirst).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(kafkaClusterFirstName, kafkaExporterTopicName, 7, 2, namespaceFirst).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(kafkaClusterFirstName, bridgeTopicName, namespaceFirst).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(namespaceFirst, kafkaClusterFirstName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(namespaceFirst, kafkaClusterFirstName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());

        coScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(TestConstants.CO_NAMESPACE, coScraperName).get(0).getMetadata().getName();
        testSuiteScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, testSuiteScraperName).get(0).getMetadata().getName();
        scraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(namespaceFirst, scraperName).get(0).getMetadata().getName();
        secondNamespaceScraperPodName = ResourceManager.kubeClient().listPodsByPrefixInName(namespaceSecond, secondScraperName).get(0).getMetadata().getName();

        // Allow connections from clients to operators pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(TestConstants.CO_NAMESPACE);

        // wait some time for metrics to be stable - at least reconciliation interval + 10s
        LOGGER.info("Sleeping for {} to give operators and operands some time to stable the metrics values before collecting",
                TestConstants.SAFETY_RECONCILIATION_INTERVAL);
        Thread.sleep(TestConstants.SAFETY_RECONCILIATION_INTERVAL);

        kafkaCollector = new MetricsCollector.Builder()
            .withScraperPodName(scraperPodName)
            .withNamespaceName(namespaceFirst)
            .withComponentType(ComponentType.Kafka)
            .withComponentName(kafkaClusterFirstName)
            .build();

        if (!Environment.isKRaftModeEnabled()) {
            zookeeperCollector = kafkaCollector.toBuilder()
                .withComponentType(ComponentType.Zookeeper)
                .build();
            zookeeperCollector.collectMetricsFromPods();
        }

        kafkaExporterCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.KafkaExporter)
            .build();

        clusterOperatorCollector = new MetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponentType(ComponentType.ClusterOperator)
            .withComponentName(clusterOperator.getClusterOperatorName())
            .build();

        kafkaCollector.collectMetricsFromPods();
        kafkaExporterCollector.collectMetricsFromPods();
        clusterOperatorCollector.collectMetricsFromPods();
    }
}
