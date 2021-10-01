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
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.GLOBAL_TIMEOUT;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.METRICS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.RECONCILIATION_INTERVAL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
@Tag(METRICS)
@Tag(CRUISE_CONTROL)
public class MetricsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);

    public static final String FIRST_NAMESPACE = "first-metrics-cluster-test";
    public static final String SECOND_NAMESPACE = "second-metrics-cluster-test";
    public static final String SECOND_CLUSTER = "second-kafka-cluster";
    public static final String MIRROR_MAKER_CLUSTER = "mm2-cluster";
    private static final String BRIDGE_CLUSTER = "my-bridge";
    private final String metricsClusterName = "metrics-cluster-name";
    private final Object lock = new Object();
    private Map<String, String> kafkaMetricsData;
    private Map<String, String> zookeeperMetricsData;
    private Map<String, String> kafkaConnectMetricsData;
    private Map<String, String> kafkaExporterMetricsData;
    private Map<String, String> kafkaMirrorMaker2MetricsData;
    private Map<String, String> kafkaBridgeMetricsData;
    private Map<String, String> clusterOperatorMetricsData;
    private Map<String, String> userOperatorMetricsData;
    private Map<String, String> topicOperatorMetricsData;
    String kafkaClientsPodName;

    private String bridgeTopic = KafkaTopicUtils.generateRandomNameOfTopic();
    private String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

    private MetricsCollector collector;

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testKafkaBrokersCount() {
        Pattern brokerOnlineCount = Pattern.compile("kafka_server_replicamanager_leadercount ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(brokerOnlineCount, kafkaMetricsData);
        assertThat("Broker count doesn't match expected value", values.size(), is(3));
    }

    @ParallelTest
    void testKafkaTopicPartitions() {
        Pattern topicPartitions = Pattern.compile("kafka_server_replicamanager_partitioncount ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(topicPartitions, kafkaMetricsData);
        assertThat("Topic partitions count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is(425.0));
    }

    @ParallelTest
    void testKafkaTopicUnderReplicatedPartitions() {
        Pattern underReplicatedPartitions = Pattern.compile("kafka_server_replicamanager_underreplicatedpartitions ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(underReplicatedPartitions, kafkaMetricsData);
        assertThat("Topic under-replicated partitions doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is((double) 0));
    }

    @ParallelTest
    void testKafkaActiveControllers() {
        Pattern activeControllers = Pattern.compile("kafka_controller_kafkacontroller_activecontrollercount ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(activeControllers, kafkaMetricsData);
        assertThat("Kafka active controllers count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testZookeeperQuorumSize() {
        Pattern quorumSize = Pattern.compile("zookeeper_quorumsize ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(quorumSize, zookeeperMetricsData);
        assertThat("Zookeeper quorum size doesn't match expected value", values.stream().mapToDouble(i -> i).max(), is(OptionalDouble.of(3.0)));
    }

    @ParallelTest
    void testZookeeperAliveConnections() {
        Pattern numAliveConnections = Pattern.compile("zookeeper_numaliveconnections ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(numAliveConnections, zookeeperMetricsData);
        assertThat("Zookeeper alive connections count doesn't match expected value", values.stream().mapToDouble(i -> i).count(), is(0L));
    }

    @ParallelTest
    void testZookeeperWatchersCount() {
        Pattern watchersCount = Pattern.compile("zookeeper_inmemorydatatree_watchcount ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(watchersCount, zookeeperMetricsData);
        assertThat("Zookeeper watchers count doesn't match expected value", values.stream().mapToDouble(i -> i).count(), is(0L));
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectRequests() {
        kafkaConnectMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.KafkaConnect)
            .build()
            .collectMetricsFromPods();
        Pattern connectRequests = Pattern.compile("kafka_connect_node_request_total\\{clientid=\".*\",} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(connectRequests, kafkaConnectMetricsData);
        assertThat("KafkaConnect requests count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectResponse() {
        kafkaConnectMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.KafkaConnect)
            .build()
            .collectMetricsFromPods();
        Pattern connectResponse = Pattern.compile("kafka_connect_node_response_total\\{clientid=\".*\",.*} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(connectResponse, kafkaConnectMetricsData);
        assertThat("KafkaConnect response count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectIoNetwork() {
        kafkaConnectMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.KafkaConnect)
            .build()
            .collectMetricsFromPods();
        Pattern connectIoNetwork = Pattern.compile("kafka_connect_network_io_total\\{clientid=\".*\",} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(connectIoNetwork, kafkaConnectMetricsData);
        assertThat("KafkaConnect IO network count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @IsolatedTest
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaExporterDataAfterExchange() {
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(FIRST_NAMESPACE)
            .withClusterName(metricsClusterName)
            .withMessageCount(5000)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        kafkaExporterMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.KafkaExporter)
            .build()
            .collectMetricsFromPods();

        TestUtils.waitFor("Kafka Exporter will contain correct metrics", Constants.GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT, () -> {
            try {
                assertThat("Kafka Exporter metrics should be non-empty", kafkaExporterMetricsData.size() > 0);
                kafkaExporterMetricsData.forEach((key, value) -> {
                    assertThat("Value from collected metric should be non-empty", !value.isEmpty());
                    assertThat("Metrics doesn't contain specific values", value.contains("kafka_consumergroup_current_offset"));
                    assertThat("Metrics doesn't contain specific values", value.contains("kafka_consumergroup_lag"));
                    assertThat("Metrics doesn't contain specific values", value.contains("kafka_topic_partitions{topic=\"" + topicName + "\"} 7"));
                });

                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @ParallelTest
    void testKafkaExporterDifferentSetting() throws InterruptedException, ExecutionException, IOException {
        LabelSelector exporterSelector = kubeClient().getDeploymentSelectors(KafkaExporterResources.deploymentName(metricsClusterName));
        String runScriptContent = getExporterRunScript(kubeClient().listPods(exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\".*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\".*\""));

        Map<String, String> kafkaExporterSnapshot = DeploymentUtils.depSnapshot(KafkaExporterResources.deploymentName(metricsClusterName));

        KafkaResource.replaceKafkaResource(metricsClusterName, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex("my-group.*");
            k.getSpec().getKafkaExporter().setTopicRegex(topicName);
        });

        DeploymentUtils.waitTillDepHasRolled(KafkaExporterResources.deploymentName(metricsClusterName), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(kubeClient().listPods(exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + topicName + "\""));
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testClusterOperatorMetrics(ExtensionContext extensionContext) {
        clusterOperatorMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.ClusterOperator)
            .withComponentName("")
            .build()
            .collectMetricsFromPods();

        List<String> resourcesList = Arrays.asList("Kafka", "KafkaBridge", "KafkaConnect", "KafkaConnector", "KafkaMirrorMaker", "KafkaMirrorMaker2", "KafkaRebalance");

        for (String resource : resourcesList) {
            assertCoMetricNotNull("strimzi_reconciliations_periodical_total", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_count", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_sum", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_max", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_locked_total", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_successful_total", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_periodical_total", resource, clusterOperatorMetricsData);
            assertCoMetricNotNull("strimzi_reconciliations_failed_total", resource, clusterOperatorMetricsData);
        }

        assertCoMetricResources(Kafka.RESOURCE_KIND, FIRST_NAMESPACE, 1);
        assertCoMetricResources(Kafka.RESOURCE_KIND, SECOND_NAMESPACE, 1);
        assertCoMetricResourceState(Kafka.RESOURCE_KIND, metricsClusterName, FIRST_NAMESPACE, 1, "none");
        assertCoMetricResourceState(Kafka.RESOURCE_KIND, SECOND_CLUSTER, SECOND_NAMESPACE, 1, "none");

        assertCoMetricResources(KafkaBridge.RESOURCE_KIND, FIRST_NAMESPACE, 1);
        assertCoMetricResources(KafkaBridge.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceState(KafkaBridge.RESOURCE_KIND, BRIDGE_CLUSTER, FIRST_NAMESPACE, 1, "none");

        assertCoMetricResources(KafkaConnect.RESOURCE_KIND, FIRST_NAMESPACE, 1);
        assertCoMetricResources(KafkaConnect.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceState(KafkaConnect.RESOURCE_KIND, metricsClusterName, FIRST_NAMESPACE, 1, "none");

        assertCoMetricResources(KafkaMirrorMaker.RESOURCE_KIND, FIRST_NAMESPACE, 0);
        assertCoMetricResources(KafkaMirrorMaker.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceStateNotExists(KafkaMirrorMaker.RESOURCE_KIND, FIRST_NAMESPACE, metricsClusterName);

        assertCoMetricResources(KafkaMirrorMaker2.RESOURCE_KIND, FIRST_NAMESPACE, 1);
        assertCoMetricResources(KafkaMirrorMaker2.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceState(KafkaMirrorMaker2.RESOURCE_KIND, MIRROR_MAKER_CLUSTER, FIRST_NAMESPACE, 1, "none");

        assertCoMetricResources(KafkaConnector.RESOURCE_KIND, FIRST_NAMESPACE, 0);
        assertCoMetricResources(KafkaConnector.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceStateNotExists(KafkaConnector.RESOURCE_KIND, FIRST_NAMESPACE, metricsClusterName);

        assertCoMetricResources(KafkaRebalance.RESOURCE_KIND, FIRST_NAMESPACE, 0);
        assertCoMetricResources(KafkaRebalance.RESOURCE_KIND, SECOND_NAMESPACE, 0);
        assertCoMetricResourceStateNotExists(KafkaRebalance.RESOURCE_KIND, FIRST_NAMESPACE, metricsClusterName);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testUserOperatorMetrics() {
        userOperatorMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.UserOperator)
            .build()
            .collectMetricsFromPods();

        assertCoMetricNotNull("strimzi_reconciliations_locked_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_successful_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_count", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_sum", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_max", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_periodical_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_failed_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_total", "KafkaUser", userOperatorMetricsData);

        Pattern userPattern = Pattern.compile("strimzi_resources\\{kind=\"KafkaUser\",.*} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(userPattern, userOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 2));
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testTopicOperatorMetrics() {
        topicOperatorMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.TopicOperator)
            .build()
            .collectMetricsFromPods();

        assertCoMetricNotNull("strimzi_reconciliations_locked_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_successful_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_count", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_sum", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_max", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_periodical_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_failed_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_total", "KafkaTopic", topicOperatorMetricsData);

        Pattern topicPattern = Pattern.compile("strimzi_resources\\{kind=\"KafkaTopic\",.*} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(topicPattern, topicOperatorMetricsData);
        cmdKubeClient().list("KafkaTopic").forEach(topicName -> {
            LOGGER.info("KafkaTopic: {}", topicName);
        });
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) getExpectedTopics().size()));
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @Tag(ACCEPTANCE)
    void testMirrorMaker2Metrics() {
        kafkaMirrorMaker2MetricsData = collector.toBuilder()
            .withComponentName(MIRROR_MAKER_CLUSTER)
            .withComponentType(ComponentType.KafkaMirrorMaker2)
            .build()
            .collectMetricsFromPods();

        Pattern connectResponse = Pattern.compile("kafka_connect_worker_connector_count ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(connectResponse, kafkaMirrorMaker2MetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 3));

        connectResponse = Pattern.compile("kafka_connect_worker_task_count ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        values = MetricsCollector.collectSpecificMetric(connectResponse, kafkaMirrorMaker2MetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @ParallelTest
    @Tag(BRIDGE)
    @Tag(ACCEPTANCE)
    void testKafkaBridgeMetrics(ExtensionContext extensionContext) {
        String producerName = "bridge-producer";
        String consumerName = "bridge-consumer";

        MetricsCollector bridgeCollector = collector.toBuilder()
            .withComponentName(BRIDGE_CLUSTER)
            .withComponentType(ComponentType.KafkaBridge)
            .build();

        // Attach consumer before producer
        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(BRIDGE_CLUSTER))
            .withTopicName(bridgeTopic)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(200)
            .withPollInterval(200)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge().build());

        TestUtils.waitFor("KafkaProducer metrics will be available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            LOGGER.info("Looking for 'strimzi_bridge_kafka_producer_count' in bridge metrics");
            kafkaBridgeMetricsData = bridgeCollector.collectMetricsFromPods();
            Pattern producerCountPattern = Pattern.compile("strimzi_bridge_kafka_producer_count\\{.*,} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
            ArrayList<Double> producerCountValues = MetricsCollector.collectSpecificMetric(producerCountPattern, kafkaBridgeMetricsData);
            return producerCountValues.stream().mapToDouble(i -> i).count() == (double) 1;
        });

        TestUtils.waitFor("KafkaConsumer metrics will be available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            LOGGER.info("Looking for 'strimzi_bridge_kafka_consumer_connection_count' in bridge metrics");
            kafkaBridgeMetricsData = bridgeCollector.collectMetricsFromPods();
            Pattern consumerConnectionsPattern = Pattern.compile("strimzi_bridge_kafka_consumer_connection_count\\{.*,} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
            ArrayList<Double> consumerConnectionsValues = MetricsCollector.collectSpecificMetric(consumerConnectionsPattern, kafkaBridgeMetricsData);
            return consumerConnectionsValues.stream().mapToDouble(i -> i).count() > 0;
        });

        assertThat("Collected KafkaBridge metrics doesn't contains jvm metrics", kafkaBridgeMetricsData.values().toString().contains("jvm"));
        assertThat("Collected KafkaBridge metrics doesn't contains HTTP metrics", kafkaBridgeMetricsData.values().toString().contains("strimzi_bridge_http_server"));

        Pattern bridgeResponse = Pattern.compile("system_cpu_count ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(bridgeResponse, kafkaBridgeMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testCruiseControlMetrics() {

        String cruiseControlMetrics = CruiseControlUtils.callApi(FIRST_NAMESPACE, CruiseControlUtils.SupportedHttpMethods.GET, "/metrics");

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
     * 1. Update metrics form whatever it is to @metricsConfigYaml in spec.kafka.metricsConfig
     * 2. Check, whether the metrics ConfigMap is changed
     * 3. Updates ConfigMap linked as metrics on
     * 4. Check, whether the metrics ConfigMap is changed
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
                .withData(Collections.singletonMap(Constants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(SECOND_NAMESPACE)
                .endMetadata()
                .build();

        kubeClient().getClient().configMaps().inNamespace(SECOND_NAMESPACE).createOrReplace(externalMetricsCm);

        // spec.kafka.metrics -> spec.kafka.jmxExporterMetrics
        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("external-metrics-cm")
                .withKey(Constants.METRICS_CONFIG_YAML_NAME)
                .build();
        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();
        KafkaResource.replaceKafkaResourceInSpecificNamespace(SECOND_CLUSTER, k -> {
            k.getSpec().getKafka().setMetricsConfig(jmxPrometheusExporterMetrics);
        }, SECOND_NAMESPACE);

        PodUtils.verifyThatRunningPodsAreStable(SECOND_NAMESPACE, SECOND_CLUSTER);
        ConfigMap actualCm = kubeClient(SECOND_NAMESPACE).getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(SECOND_CLUSTER));
        assertThat(actualCm.getData().get(Constants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson));

        // update metrics
        ConfigMap externalMetricsUpdatedCm = new ConfigMapBuilder()
                .withData(Collections.singletonMap(Constants.METRICS_CONFIG_YAML_NAME, metricsConfigYaml.replace("true", "false")))
                .withNewMetadata()
                    .withName("external-metrics-cm")
                    .withNamespace(SECOND_NAMESPACE)
                .endMetadata()
                .build();

        kubeClient().getClient().configMaps().inNamespace(SECOND_NAMESPACE).createOrReplace(externalMetricsUpdatedCm);
        PodUtils.verifyThatRunningPodsAreStable(SECOND_NAMESPACE, SECOND_CLUSTER);
        actualCm = kubeClient(SECOND_NAMESPACE).getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(SECOND_CLUSTER));
        assertThat(actualCm.getData().get(Constants.METRICS_CONFIG_JSON_NAME), is(metricsConfigJson.replace("true", "false")));
    }

    @IsolatedTest
    void testReconcileStateMetricInTopicOperator(ExtensionContext extensionContext) {
        cluster.setNamespace(SECOND_NAMESPACE);

        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int initialReplicas = 1;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(SECOND_CLUSTER, topicName, 1, initialReplicas).build());

        String secondClientsPodName = kubeClient(SECOND_NAMESPACE).listPodsByPrefixInName(SECOND_NAMESPACE + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        MetricsCollector secondNamespaceCollector = new MetricsCollector.Builder()
            .withNamespaceName(SECOND_NAMESPACE)
            .withScraperPodName(secondClientsPodName)
            .withComponentName(SECOND_CLUSTER)
            .withComponentType(ComponentType.TopicOperator)
            .build();

        Map<String, String> toMetrics = secondNamespaceCollector.collectMetricsFromPods();
        String reasonMessage = "none";

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertResourceStateMetricInTopicOperator(topicName, reasonMessage, 1.0, toMetrics);

        LOGGER.info("Changing topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResource(topicName, kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"));
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);

        toMetrics = secondNamespaceCollector.collectMetricsFromPods();
        reasonMessage = "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.";
        assertResourceStateMetricInTopicOperator(topicName, reasonMessage, 0.0, toMetrics);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResource(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(topicName);
            kafkaTopic.getSpec().setReplicas(12);
        });

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(SECOND_NAMESPACE, topicName, 12);

        toMetrics = secondNamespaceCollector.collectMetricsFromPods();
        reasonMessage = "Changing 'spec.replicas' is not supported. .*";
        assertResourceStateMetricInTopicOperator(topicName, reasonMessage, 0.0, toMetrics);

        LOGGER.info("Scaling replicas to be higher than before");
        KafkaTopicResource.replaceTopicResource(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(24));

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(SECOND_NAMESPACE, topicName, 24);

        toMetrics = secondNamespaceCollector.collectMetricsFromPods();
        assertResourceStateMetricInTopicOperator(topicName, reasonMessage, 0.0, toMetrics);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResource(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas));
        KafkaTopicUtils.waitForKafkaTopicReady(topicName);

        toMetrics = secondNamespaceCollector.collectMetricsFromPods();
        reasonMessage = "none";
        assertResourceStateMetricInTopicOperator(topicName, reasonMessage, 1.0, toMetrics);

        cluster.setNamespace(FIRST_NAMESPACE);
    }

    private String getExporterRunScript(String podName) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("cat");
        command.add("/tmp/run.sh");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", FIRST_NAMESPACE, "--"));
        executableCommand.addAll(command);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the pod
        int ret = exec.execute(null, executableCommand, 20_000);

        synchronized (lock) {
            LOGGER.info("Metrics collection for pod {} return code - {}", podName, ret);
        }

        assertThat("Collected metrics should not be empty", exec.out(), not(emptyString()));
        return exec.out();
    }

    private void assertResourceStateMetricInTopicOperator(String topicName, String reasonMessage, Double resultValue, Map<String, String> data) {
        Pattern reconcileStateMetric =
            Pattern.compile("strimzi_resource_state\\{kind=\"KafkaTopic\",name=\"" + topicName + "\",reason=\"" + reasonMessage + "\",resource_namespace=\"" + SECOND_NAMESPACE + "\",} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(reconcileStateMetric, data);
        assertEquals(1, values.size());
        assertEquals(values.get(0), resultValue);
    }

    private void assertCoMetricNotNull(String metric, String kind, Map<String, String> data) {
        Pattern connectResponse = Pattern.compile(metric + "\\{kind=\"" + kind + "\",} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(connectResponse, data);
        assertThat(values.stream().mapToDouble(i -> i).count(), notNullValue());
    }

    private void assertCoMetricResourceStateNotExists(String kind, String name, String namespace) {
        Pattern pattern = Pattern.compile("strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",resource_namespace=\"" + namespace + "\",} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(pattern, clusterOperatorMetricsData);
        assertThat(values.isEmpty(), is(true));
    }

    private void assertCoMetricResourceState(String kind, String name, String namespace, int value, String reason) {
        Pattern pattern = Pattern.compile("strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",reason=\"" + reason + "\",resource_namespace=\"" + namespace + "\",} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(pattern, clusterOperatorMetricsData);
        assertThat("strimzi_resource_state for " + kind + " is not " + value, values.stream().mapToDouble(i -> i).sum(), is((double) value));
    }

    private void assertCoMetricResources(String kind, String namespace, int value) {
        Pattern pattern = Pattern.compile("strimzi_resources\\{kind=\"" + kind + "\",namespace=\"" + namespace + "\",.*} ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        ArrayList<Double> values = MetricsCollector.collectSpecificMetric(pattern, clusterOperatorMetricsData);
        assertThat("strimzi_resources for " + kind + " is not " + value, values.stream().mapToDouble(i -> i).sum(), is((double) value));
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) throws Exception {
        LOGGER.info("Setting up Environment for MetricsST");

        cluster.createNamespace(SECOND_NAMESPACE);
        cluster.setNamespace(SECOND_NAMESPACE);

        NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, Collections.singletonList(SECOND_NAMESPACE));

        cluster.setNamespace(FIRST_NAMESPACE);

        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(FIRST_NAMESPACE)
            .withWatchingNamespaces(FIRST_NAMESPACE + "," + SECOND_NAMESPACE)
            .withBindingsNamespaces(List.of(FIRST_NAMESPACE, SECOND_NAMESPACE))
            .createInstallation()
            .runInstallation();

        final String firstKafkaClientsName = FIRST_NAMESPACE + "-" + Constants.KAFKA_CLIENTS;
        final String secondKafkaClientsName = SECOND_NAMESPACE + "-" + Constants.KAFKA_CLIENTS;

        // create resources without wait to deploy them simultaneously
        resourceManager.createResource(extensionContext, false,
            // kafka with cruise control and metrics
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(metricsClusterName, 3, 3)
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
            KafkaTemplates.kafkaWithMetrics(SECOND_CLUSTER, SECOND_NAMESPACE, 1, 1).build(),
            KafkaClientsTemplates.kafkaClients(FIRST_NAMESPACE, false, firstKafkaClientsName).build(),
            KafkaClientsTemplates.kafkaClients(SECOND_NAMESPACE, false, secondKafkaClientsName).build(),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2WithMetrics(MIRROR_MAKER_CLUSTER, metricsClusterName, SECOND_CLUSTER, 1).build(),
            KafkaBridgeTemplates.kafkaBridgeWithMetrics(BRIDGE_CLUSTER, metricsClusterName, KafkaResources.plainBootstrapAddress(metricsClusterName), 1).build()
        );

        // sync resources
        resourceManager.synchronizeResources(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(metricsClusterName, topicName, 7, 2).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(metricsClusterName, bridgeTopic).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(metricsClusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(metricsClusterName, KafkaUserUtils.generateRandomNameOfKafkaUser()).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnectWithMetrics(metricsClusterName, 1).build());

        kafkaClientsPodName = ResourceManager.kubeClient().listPodsByPrefixInName(firstKafkaClientsName).get(0).getMetadata().getName();

        // Allow connections from clients to operators pods when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForClusterOperator(extensionContext);
        NetworkPolicyResource.allowNetworkPolicySettingsForEntityOperator(extensionContext, metricsClusterName);
        NetworkPolicyResource.allowNetworkPolicySettingsForEntityOperator(extensionContext, SECOND_CLUSTER, SECOND_NAMESPACE);
        // Allow connections from clients to KafkaExporter when NetworkPolicies are set to denied by default
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, metricsClusterName);
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, SECOND_CLUSTER, SECOND_NAMESPACE);

        // wait some time for metrics to be stable - at least reconciliation interval + 10s
        Thread.sleep(RECONCILIATION_INTERVAL + 10_000);

        collector = new MetricsCollector.Builder()
            .withScraperPodName(kafkaClientsPodName)
            .withComponentType(ComponentType.Kafka)
            .withComponentName(metricsClusterName)
            .build();

        kafkaMetricsData = collector.collectMetricsFromPods();

        zookeeperMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.Zookeeper)
            .build()
            .collectMetricsFromPods();

        kafkaConnectMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.KafkaConnect)
            .build()
            .collectMetricsFromPods();

        kafkaExporterMetricsData = collector.toBuilder()
            .withComponentType(ComponentType.KafkaExporter)
            .build()
            .collectMetricsFromPods();
    }

    private List<String> getExpectedTopics() {
        ArrayList<String> list = new ArrayList<>();
        list.add("mirrormaker2-cluster-configs");
        list.add("mirrormaker2-cluster-offsets");
        list.add("mirrormaker2-cluster-status");
        list.add("mm2-offset-syncs.my-cluster.internal");
        list.add(metricsClusterName + "-connect-config");
        list.add(metricsClusterName + "-connect-offsets");
        list.add(metricsClusterName + "-connect-status");
        list.add("heartbeats");
        list.add(topicName);
        list.add(bridgeTopic);
        list.add(CruiseControlUtils.CRUISE_CONTROL_METRICS_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        list.add("strimzi-store-topic");
        list.add("strimzi-topic-operator-kstreams-topic-store-changelog");
        return list;
    }
}
