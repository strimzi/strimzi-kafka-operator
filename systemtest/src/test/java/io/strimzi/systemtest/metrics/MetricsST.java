/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
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
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
public class MetricsST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);

    public static final String NAMESPACE = "metrics-cluster-test";
    private final Object lock = new Object();
    private HashMap<String, String> kafkaMetricsData;
    private HashMap<String, String> zookeeperMetricsData;
    private HashMap<String, String> kafkaConnectMetricsData;
    private HashMap<String, String> kafkaExporterMetricsData;

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
        assertThat("Topic partitions count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is(257.0));
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
    void testKafkaConnectRequests() {
        Pattern connectRequests = Pattern.compile("kafka_connect_connect_metrics_connect_1_request_total ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectRequests, kafkaConnectMetricsData);
        assertThat("Kafka Connect requests count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    void testKafkaConnectResponse() {
        Pattern connectResponse = Pattern.compile("kafka_connect_connect_metrics_connect_1_response_total ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectResponse, kafkaConnectMetricsData);
        assertThat("Kafka Connect response count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    void testKafkaConnectIoNetwork() {
        Pattern connectIoNetwork = Pattern.compile("kafka_connect_connect_metrics_connect_1_network_io_total ([\\d.][^\\n]+)");
        ArrayList<Double> values = MetricsUtils.collectSpecificMetric(connectIoNetwork, kafkaConnectMetricsData);
        assertThat("Kafka Connect IO network count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    void testKafkaExporterDataAfterExchange() throws InterruptedException {
        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName("my-cluster" + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessages(TEST_TOPIC_NAME, NAMESPACE, CLUSTER_NAME, 5000),
            internalKafkaClient.receiveMessages(TEST_TOPIC_NAME, NAMESPACE, CLUSTER_NAME, 5000, CONSUMER_GROUP_NAME)
        );

        kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(CLUSTER_NAME);
        assertThat("Kafka Exporter metrics should be non-empty", kafkaExporterMetricsData.size() > 0);
        kafkaExporterMetricsData.forEach((key, value) -> {
            assertThat("Value from collected metric should be non-empty", !value.isEmpty());
            assertThat("Metrics doesn't contain specific values", value.contains("kafka_consumergroup_current_offset"));
            assertThat("Metrics doesn't contain specific values", value.contains("kafka_consumergroup_lag"));
            assertThat("Metrics doesn't contain specific values", value.contains("kafka_topic_partitions{topic=\"" + TEST_TOPIC_NAME + "\"} 7"));
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
            k.getSpec().getKafkaExporter().setTopicRegex(TEST_TOPIC_NAME);
        });

        DeploymentUtils.waitTillDepHasRolled(KafkaExporterResources.deploymentName(CLUSTER_NAME), 1, kafkaExporterSnapshot);

        runScriptContent = getExporterRunScript(kubeClient().listPods(exporterSelector).get(0).getMetadata().getName());
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--group.filter=\"my-group.*\""));
        assertThat("Exporter starting script has wrong setting than it's specified in CR", runScriptContent.contains("--topic.filter=\"" + TEST_TOPIC_NAME + "\""));
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

    // No need to recreate environment after failed test. Only values from collected metrics are checked
    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) { }

    @BeforeAll
    void setupEnvironment() throws InterruptedException {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
        KafkaResource.kafkaWithMetrics(CLUSTER_NAME, 3, 3).done();
        KafkaConnectResource.kafkaConnectWithMetrics(CLUSTER_NAME, 1).done();
        KafkaTopicResource.topic(CLUSTER_NAME, "test-topic", 7, 2).done();
        // Wait for Metrics refresh/values change
        Thread.sleep(60_000);
        kafkaMetricsData = MetricsUtils.collectKafkaPodsMetrics(CLUSTER_NAME);
        zookeeperMetricsData = MetricsUtils.collectZookeeperPodsMetrics(CLUSTER_NAME);
        kafkaConnectMetricsData = MetricsUtils.collectKafkaConnectPodsMetrics(CLUSTER_NAME);
        kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(CLUSTER_NAME);
    }
}
