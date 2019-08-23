/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

public class MetricsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MetricsST.class);

    public static final String NAMESPACE = "metrics-cluster-test";
    private final Object lock = new Object();
    private HashMap<String, String> kafkaMetricsData;
    private HashMap<String, String> zookeeperMetricsData;
    private HashMap<String, String> kafkaConnectMetricsData;

    @Test
    void testKafkaBrokersCount() {
        Pattern brokerOnlineCount = Pattern.compile("kafka_server_replicamanager_leadercount ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(brokerOnlineCount, kafkaMetricsData);
        assertThat("Broker count doesn't match expected value", values.size(), is(3));
    }

    @Test
    void testKafkaTopicPartitions() {
        Pattern topicPartitions = Pattern.compile("kafka_server_replicamanager_partitioncount ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(topicPartitions, kafkaMetricsData);
        assertThat("Topic partitions count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is(257.0));
    }

    @Test
    void testKafkaTopicUnderReplicatedPartitions() {
        Pattern underReplicatedPartitions = Pattern.compile("kafka_server_replicamanager_underreplicatedpartitions ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(underReplicatedPartitions, kafkaMetricsData);
        assertThat("Topic under-replicated partitions doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is((double) 0));
    }

    @Test
    void testKafkaActiveControllers() {
        Pattern activeControllers = Pattern.compile("kafka_controller_kafkacontroller_activecontrollercount ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(activeControllers, kafkaMetricsData);
        assertThat("Kafka active controllers count doesn't match expected value", values.stream().mapToDouble(i -> i).sum(), is((double) 1));
    }

    @Test
    void testZookeeperQuorumSize() {
        Pattern quorumSize = Pattern.compile("zookeeper_quorumsize ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(quorumSize, zookeeperMetricsData);
        assertThat("Zookeeper quorum size doesn't match expected value", values.stream().mapToDouble(i -> i).max(), is(OptionalDouble.of(3.0)));
    }

    @Test
    void testZookeeperAliveConnections() {
        Pattern numAliveConnections = Pattern.compile("zookeeper_numaliveconnections ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(numAliveConnections, zookeeperMetricsData);
        assertThat("Zookeeper alive connections count doesn't match expected value", values.stream().mapToDouble(i -> i).count(), is(0L));
    }

    @Test
    void testZookeeperWatchersCount() {
        Pattern watchersCount = Pattern.compile("zookeeper_inmemorydatatree_watchcount ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(watchersCount, zookeeperMetricsData);
        assertThat("Zookeeper watchers count doesn't match expected value", values.stream().mapToDouble(i -> i).count(), is(0L));
    }

    @Test
    void testKafkaConnectRequests() {
        Pattern connectRequests = Pattern.compile("kafka_connect_connect_metrics_connect_1_request_total ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(connectRequests, kafkaConnectMetricsData);
        assertThat("Kafka Connect requests count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    void testKafkaConnectResponse() {
        Pattern connectResponse = Pattern.compile("kafka_connect_connect_metrics_connect_1_response_total ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(connectResponse, kafkaConnectMetricsData);
        assertThat("Kafka Connect response count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    @Test
    void testKafkaConnectIoNetwork() {
        Pattern connectIoNetwork = Pattern.compile("kafka_connect_connect_metrics_connect_1_network_io_total ([\\d.][^\\n]+)");
        ArrayList<Double> values = collectSpecificMetric(connectIoNetwork, kafkaConnectMetricsData);
        assertThat("Kafka Connect IO network count doesn't match expected value", values.stream().mapToDouble(i -> i).sum() > 0);
    }

    private HashMap<String, String> collectKafkaPodsMetrics() {
        LabelSelector kafkaSelector = kubeClient().getStatefulSetSelectors(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        return collectMetricsFromPods(kafkaSelector);
    }

    private HashMap<String, String> collectZookeeperPodsMetrics() {
        LabelSelector zookeeperSelector = kubeClient().getStatefulSetSelectors(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        return collectMetricsFromPods(zookeeperSelector);
    }

    private HashMap<String, String> collectKafkaConnectPodsMetrics() {
        LabelSelector connectSelector = kubeClient().getDeploymentSelectors(KafkaConnectResources.deploymentName(CLUSTER_NAME));
        return collectMetricsFromPods(connectSelector);
    }


    /**
     * Parse out specific metric from whole metrics file
     * @param pattern regex patern for specific metric
     * @param data all metrics data
     * @return list of parsed values
     */
    private ArrayList<Double> collectSpecificMetric(Pattern pattern, HashMap<String, String> data) {
        ArrayList<Double> values = new ArrayList<>();

        data.forEach((k, v) -> {
            Matcher t = pattern.matcher(v);
            if (t.find()) {
                values.add(Double.parseDouble(t.group(1)));
            }
        });
        return values;
    }

    /**
     * Collect metrics from all pods with specific selector
     * @param labelSelector pod selector
     * @return map with metrics {podName, metrics}
     */
    private HashMap<String, String> collectMetricsFromPods(LabelSelector labelSelector) {
        HashMap<String, String> map = new HashMap<>();
        kubeClient().listPods(labelSelector).forEach(p -> {
            try {
                map.put(p.getMetadata().getName(), collectMetrics(p.getMetadata().getName()));
            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }
        });

        return  map;
    }

    /**
     * Collect metrics from specific pod
     * @param podName pod name
     * @return collected metrics
     */
    private String collectMetrics(String podName) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("curl");
        command.add(kubeClient().getPod(podName).getStatus().getPodIP() + ":9404");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", NAMESPACE, "--"));
        executableCommand.addAll(command);

        Exec exec = new Exec();
        int ret = exec.execute(null, executableCommand, 20000);

        synchronized (lock) {
            LOGGER.info("Metrics collection for pod {} return code - {}", podName, ret);
        }

        assertThat("Collected metrics should not be empty", exec.out(), not(isEmptyString()));
        return exec.out();
    }

    // No need to recreate environment after failed test. Only values from collected metrics are checked
    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) { }

    @BeforeAll
    void setupEnvironment() throws InterruptedException {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
        testClassResources().kafkaWithMetrics(CLUSTER_NAME, 3, 3).done();
        testClassResources().kafkaConnectWithMetrics(CLUSTER_NAME, 1).done();
        testClassResources().topic(CLUSTER_NAME, "test-topic", 7, 2).done();
        // Wait for Metrics refresh/values change
        Thread.sleep(60000);
        kafkaMetricsData = collectKafkaPodsMetrics();
        zookeeperMetricsData = collectZookeeperPodsMetrics();
        kafkaConnectMetricsData = collectKafkaConnectPodsMetrics();
    }
}
