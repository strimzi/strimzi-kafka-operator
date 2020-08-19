/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.METRICS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
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

@Tag(REGRESSION)
@Tag(ACCEPTANCE)
@Tag(METRICS)
public class MetricsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);

    public static final String NAMESPACE = "metrics-cluster-test";
    public static final String SECOND_CLUSTER = "second-kafka-cluster";
    public static final String MIRROR_MAKER_CLUSTER = "mm2-cluster";
    private static final String BRIDGE_CLUSTER = "my-bridge";
    private final Object lock = new Object();
    private HashMap<String, String> kafkaMetricsData;
    private HashMap<String, String> zookeeperMetricsData;
    private HashMap<String, String> kafkaConnectMetricsData;
    private HashMap<String, String> kafkaExporterMetricsData;
    private HashMap<String, String> kafkaMirrorMaker2MetricsData;
    private HashMap<String, String> kafkaBridgeMetricsData;
    private HashMap<String, String> clusterOperatorMetricsData;
    private HashMap<String, String> userOperatorMetricsData;
    private HashMap<String, String> topicOperatorMetricsData;

    private String bridgeTopic = "bridge-topic";

    @Test
    void testKafkaBrokersCount() {
        Pattern brokerOnlineCount = Pattern.compile("kafka_server_replicamanager_leadercount ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(brokerOnlineCount, kafkaMetricsData);
        assertThat("Broker count doesn't match expected value", values.size(), is(3));
    }

    @Test
    void testKafkaTopicPartitions() {
        Pattern topicPartitions = Pattern.compile("kafka_server_replicamanager_partitioncount ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(topicPartitions, kafkaMetricsData);
        assertThat("Topic partitions count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is(420.0));
    }

    @Test
    void testKafkaTopicUnderReplicatedPartitions() {
        Pattern underReplicatedPartitions = Pattern.compile("kafka_server_replicamanager_underreplicatedpartitions ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(underReplicatedPartitions, kafkaMetricsData);
        assertThat("Topic under-replicated partitions doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is((double) 0));
    }

    @Test
    void testKafkaActiveControllers() {
        Pattern activeControllers = Pattern.compile("kafka_controller_kafkacontroller_activecontrollercount ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(activeControllers, kafkaMetricsData);
        assertThat("Kafka active controllers count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @Test
    void testZookeeperQuorumSize() {
        Pattern quorumSize = Pattern.compile("zookeeper_quorumsize ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(quorumSize, zookeeperMetricsData);
        assertThat("Zookeeper quorum size doesn't match expected value", values.stream().mapToDouble(i -> i).max(), is(OptionalDouble.of(3.0)));
    }

    @Test
    void testZookeeperAliveConnections() {
        Pattern numAliveConnections = Pattern.compile("zookeeper_numaliveconnections ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(numAliveConnections, zookeeperMetricsData);
        assertThat("Zookeeper alive connections count doesn't match expected value", values.stream().mapToDouble(i -> i).count(), is(0L));
    }

    @Test
    void testZookeeperWatchersCount() {
        Pattern watchersCount = Pattern.compile("zookeeper_inmemorydatatree_watchcount ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(watchersCount, zookeeperMetricsData);
        assertThat("Zookeeper watchers count doesn't match expected value", values.stream().mapToDouble(i -> i).count(), is(0L));
    }

    @Test
    @Tag(CONNECT)
    void testKafkaConnectRequests() {
        Pattern connectRequests = Pattern.compile("kafka_connect_node_request_total\\{clientid=\".*\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectRequests, kafkaConnectMetricsData);
        assertThat("KafkaConnect requests count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    @Tag(CONNECT)
    void testKafkaConnectResponse() {
        Pattern connectResponse = Pattern.compile("kafka_connect_node_response_total\\{clientid=\".*\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectResponse, kafkaConnectMetricsData);
        assertThat("KafkaConnect response count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    @Tag(CONNECT)
    void testKafkaConnectIoNetwork() {
        Pattern connectIoNetwork = Pattern.compile("kafka_connect_network_io_total\\{clientid=\".*\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectIoNetwork, kafkaConnectMetricsData);
        assertThat("KafkaConnect IO network count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaExporterDataAfterExchange() {
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(5000)
            .build();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(CLUSTER_NAME);
        assertThat("Kafka Exporter metrics should be non-empty", kafkaExporterMetricsData.size() > 0);
        kafkaExporterMetricsData.forEach((key, value) -> {
            assertThat("Value from collected metric should be non-empty", !value.isEmpty());
            assertThat("Metrics doesn't contain specific values", value.contains("kafka_consumergroup_current_offset"));
            assertThat("Metrics doesn't contain specific values", value.contains("kafka_consumergroup_lag"));
            assertThat("Metrics doesn't contain specific values", value.contains("kafka_topic_partitions{topic=\"" + TOPIC_NAME + "\"} 7"));
        });
    }

    @Test
    void testKafkaExporterDifferentSetting() throws InterruptedException, ExecutionException, IOException {
        LabelSelector exporterSelector = kubeClient().getDeploymentSelectors(KafkaExporterResources.deploymentName(CLUSTER_NAME));
        String runScriptContent = getExporterRunScript(kubeClient().listPods(exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\".*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\".*\""));

        Map<String, String> kafkaExporterSnapshot = DeploymentUtils.depSnapshot(KafkaExporterResources.deploymentName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafkaExporter().setGroupRegex("my-group.*");
            k.getSpec().getKafkaExporter().setTopicRegex(TOPIC_NAME);
        });

        DeploymentUtils.waitTillDepHasRolled(KafkaExporterResources.deploymentName(CLUSTER_NAME), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(kubeClient().listPods(exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + TOPIC_NAME + "\""));
    }

    @Test
    void testClusterOperatorMetrics() {
        clusterOperatorMetricsData = MetricsUtils.collectClusterOperatorPodMetrics();
        List<String> resourcesList = Arrays.asList("Kafka", "KafkaBridge", "KafkaConnect", "KafkaConnectS2I", "KafkaConnector", "KafkaMirrorMaker", "KafkaMirrorMaker2");

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

        Pattern connectResponse = Pattern.compile("strimzi_resources\\{kind=\"Kafka\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 2));

        connectResponse = Pattern.compile("strimzi_resources\\{kind=\"KafkaBridge\",} ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));

        connectResponse = Pattern.compile("strimzi_resources\\{kind=\"KafkaConnect\",} ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));

        connectResponse = Pattern.compile("strimzi_resources\\{kind=\"KafkaConnectS2I\",} ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 0));

        connectResponse = Pattern.compile("strimzi_resources\\{kind=\"KafkaMirrorMaker\",} ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 0));

        connectResponse = Pattern.compile("strimzi_resources\\{kind=\"KafkaMirrorMaker2\",} ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));

        connectResponse = Pattern.compile("strimzi_resources\\{kind=\"KafkaConnector\",} ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, clusterOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 0));
    }

    @Test
    void testUserOperatorMetrics() {
        userOperatorMetricsData = MetricsUtils.collectUserOperatorPodMetrics(CLUSTER_NAME);
        assertCoMetricNotNull("strimzi_reconciliations_locked_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_successful_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_count", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_sum", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_max", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_periodical_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_failed_total", "KafkaUser", userOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_total", "KafkaUser", userOperatorMetricsData);

        Pattern userPattern = Pattern.compile("strimzi_resources\\{kind=\"KafkaUser\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(userPattern, userOperatorMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 2));
    }

    @Test
    void testTopicOperatorMetrics() {
        topicOperatorMetricsData = MetricsUtils.collectTopicOperatorPodMetrics(CLUSTER_NAME);
        assertCoMetricNotNull("strimzi_reconciliations_locked_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_successful_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_count", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_sum", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_duration_seconds_max", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_periodical_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_failed_total", "KafkaTopic", topicOperatorMetricsData);
        assertCoMetricNotNull("strimzi_reconciliations_total", "KafkaTopic", topicOperatorMetricsData);

        Pattern topicPattern = Pattern.compile("strimzi_resources\\{kind=\"KafkaTopic\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(topicPattern, topicOperatorMetricsData);
        cmdKubeClient().list("KafkaTopic").stream().forEach(topicName -> {
            LOGGER.info("KafkaTopic: {}", topicName);
        });
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) getExpectedTopics().size()));
    }

    @Test
    @Tag(MIRROR_MAKER2)
    void testMirrorMaker2Metrics() {
        kafkaMirrorMaker2MetricsData = MetricsUtils.collectKafkaMirrorMaker2PodsMetrics(MIRROR_MAKER_CLUSTER);
        Pattern connectResponse = Pattern.compile("kafka_connect_worker_connector_count ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectResponse, kafkaMirrorMaker2MetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 3));

        connectResponse = Pattern.compile("kafka_connect_worker_task_count ([\\d.][^\\n]+)");
        values = MetricsUtils.collectSpecificMetric(connectResponse, kafkaMirrorMaker2MetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @Test
    @Tag(BRIDGE)
    void testKafkaBridgeMetrics() {
        String producerName = "bridge-producer";
        String consumerName = "bridge-consumer";

        // Attach consumer before producer
        KafkaClientsResource.consumerStrimziBridge(consumerName, KafkaBridgeResources.serviceName(BRIDGE_CLUSTER), Constants.HTTP_BRIDGE_DEFAULT_PORT, bridgeTopic, 200).done();
        KafkaClientsResource.producerStrimziBridge(producerName, KafkaBridgeResources.serviceName(BRIDGE_CLUSTER), Constants.HTTP_BRIDGE_DEFAULT_PORT, bridgeTopic, 200).done();

        TestUtils.waitFor("KafkaProducer metrics will be available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            LOGGER.info("Looking for 'strimzi_bridge_kafka_producer_count' in bridge metrics");
            kafkaBridgeMetricsData = MetricsUtils.collectKafkaBridgePodMetrics(BRIDGE_CLUSTER);
            Pattern producerCountPattern = Pattern.compile("strimzi_bridge_kafka_producer_count\\{.*,} ([\\d.][^\\n]+)");
            ArrayList<Double> producerCountValues = MetricsUtils.collectSpecificMetric(producerCountPattern, kafkaBridgeMetricsData);
            return producerCountValues.stream().mapToDouble(i -> i).count() == (double) 1;
        });

        TestUtils.waitFor("KafkaConsumer metrics will be available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            LOGGER.info("Looking for 'strimzi_bridge_kafka_consumer_connection_count' in bridge metrics");
            kafkaBridgeMetricsData = MetricsUtils.collectKafkaBridgePodMetrics(BRIDGE_CLUSTER);
            Pattern consumerConnectionsPattern = Pattern.compile("strimzi_bridge_kafka_consumer_connection_count\\{.*,} ([\\d.][^\\n]+)");
            ArrayList<Double> consumerConnectionsValues = MetricsUtils.collectSpecificMetric(consumerConnectionsPattern, kafkaBridgeMetricsData);
            return consumerConnectionsValues.stream().mapToDouble(i -> i).count() > 0;
        });

        assertThat("Collected KafkaBridge metrics doesn't contains jvm metrics", kafkaBridgeMetricsData.values().toString().contains("jvm"));
        assertThat("Collected KafkaBridge metrics doesn't contains HTTP metrics", kafkaBridgeMetricsData.values().toString().contains("strimzi_bridge_http_server"));

        Pattern bridgeResponse = Pattern.compile("system_cpu_count ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(bridgeResponse, kafkaBridgeMetricsData);
        assertThat(values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @Test
    @Tag(CRUISE_CONTROL)
    void testCruiseControlMetrics() {

        String cruiseControlMetrics = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.GET, "/metrics");

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

    private String getExporterRunScript(String podName) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("cat");
        command.add("/tmp/run.sh");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", NAMESPACE, "--"));
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

    private void assertCoMetricNotNull(String metric, String kind, HashMap<String, String> data) {
        Pattern connectResponse = Pattern.compile(metric + "\\{kind=\"" + kind + "\",} ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectResponse, data);
        assertThat(values.stream().mapToDouble(i -> i).count(), notNullValue());
    }

    @BeforeAll
    void setupEnvironment() throws Exception {
        LOGGER.info("Setting up Environment for MetricsST");
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        KafkaResource.kafkaWithMetricsAndCruiseControlWithMetrics(CLUSTER_NAME, 3, 3)
            .editOrNewSpec()
                .editEntityOperator()
                    .editUserOperator()
                        .withReconciliationIntervalSeconds(30)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        KafkaResource.kafkaWithMetrics(SECOND_CLUSTER, 1, 1).done();
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        KafkaConnectResource.kafkaConnectWithMetrics(CLUSTER_NAME, 1).done();
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2WithMetrics(MIRROR_MAKER_CLUSTER, CLUSTER_NAME, SECOND_CLUSTER, 1).done();
        KafkaBridgeResource.kafkaBridgeWithMetrics(BRIDGE_CLUSTER, CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME, 7, 2).done();
        KafkaTopicResource.topic(CLUSTER_NAME, bridgeTopic).done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, KafkaUserUtils.generateRandomNameOfKafkaUser()).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, KafkaUserUtils.generateRandomNameOfKafkaUser()).done();

        // Wait for Metrics refresh/values change
        Thread.sleep(60_000);
        kafkaMetricsData = MetricsUtils.collectKafkaPodsMetrics(CLUSTER_NAME);
        zookeeperMetricsData = MetricsUtils.collectZookeeperPodsMetrics(CLUSTER_NAME);
        kafkaConnectMetricsData = MetricsUtils.collectKafkaConnectPodsMetrics(CLUSTER_NAME);
        kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(CLUSTER_NAME);
        kafkaBridgeMetricsData = MetricsUtils.collectKafkaBridgePodMetrics(BRIDGE_CLUSTER);
    }
    
    private List<String> getExpectedTopics() {
        ArrayList<String> list = new ArrayList<>();
        list.add("mirrormaker2-cluster-configs");
        list.add("mirrormaker2-cluster-offsets");
        list.add("mirrormaker2-cluster-status");
        list.add("mm2-offset-syncs.my-cluster.internal");
        list.add("my-cluster-connect-config");
        list.add("my-cluster-connect-offsets");
        list.add("my-cluster-connect-status");
        list.add("second-kafka-cluster.checkpoints.internal");
        list.add("heartbeats");
        list.add(TOPIC_NAME);
        list.add(bridgeTopic);
        list.add(CruiseControlUtils.CRUISE_CONTROL_METRICS_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        list.add(CruiseControlUtils.CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        return list;
    }
}
