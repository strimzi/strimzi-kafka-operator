/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class MetricsUtils {

    private static final Logger LOGGER = LogManager.getLogger(MetricsUtils.class);
    private static final Object LOCK = new Object();

    private MetricsUtils() { }

    /**
     * Collect metrics from specific pod
     * @param scraperPodName pod name
     * @param metricsPath endpoint where metrics should be available
     * @return collected metrics
     */
    public static String collectMetrics(String scraperPodName, String metricsPodIp, int port, String metricsPath) throws InterruptedException, ExecutionException, IOException {
        List<String> executableCommand = Arrays.asList(cmdKubeClient().toString(), "exec", scraperPodName,
                "-n", kubeClient().getNamespace(),
                "--", "curl", metricsPodIp + ":" + port + metricsPath);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the pod
        int ret = exec.execute(null, executableCommand, 20_000);

        synchronized (LOCK) {
            LOGGER.info("Metrics collection for PodIp {} from Pod {} finished with return code: {}", metricsPodIp, scraperPodName, ret);
        }
        return exec.out();
    }

    public static HashMap<String, String> collectKafkaPodsMetrics(String scraperPodName, String clusterName) {
        LabelSelector kafkaSelector = kubeClient().getStatefulSetSelectors(KafkaResources.kafkaStatefulSetName(clusterName));
        return collectMetricsFromPods(scraperPodName, kafkaSelector, Constants.COMPONENTS_METRICS_PORT);
    }

    public static HashMap<String, String> collectZookeeperPodsMetrics(String scraperPodName, String clusterName) {
        LabelSelector zookeeperSelector = kubeClient().getStatefulSetSelectors(KafkaResources.zookeeperStatefulSetName(clusterName));
        return collectMetricsFromPods(scraperPodName, zookeeperSelector, Constants.COMPONENTS_METRICS_PORT);
    }

    public static HashMap<String, String> collectKafkaConnectPodsMetrics(String scraperPodName, String clusterName) {
        LabelSelector connectSelector = kubeClient().getDeploymentSelectors(KafkaConnectResources.deploymentName(clusterName));
        return collectMetricsFromPods(scraperPodName, connectSelector, Constants.COMPONENTS_METRICS_PORT);
    }

    public static HashMap<String, String> collectKafkaExporterPodsMetrics(String scraperPodName, String clusterName) {
        LabelSelector kafkaExporterSelector = kubeClient().getDeploymentSelectors(KafkaExporterResources.deploymentName(clusterName));
        return collectMetricsFromPods(scraperPodName, kafkaExporterSelector, Constants.COMPONENTS_METRICS_PORT, "/metrics");
    }

    public static HashMap<String, String> collectKafkaMirrorMaker2PodsMetrics(String scraperPodName, String clusterName) {
        LabelSelector mm2Selector = kubeClient().getDeploymentSelectors(KafkaMirrorMaker2Resources.deploymentName(clusterName));
        return collectMetricsFromPods(scraperPodName, mm2Selector, Constants.COMPONENTS_METRICS_PORT);
    }

    public static HashMap<String, String> collectUserOperatorPodMetrics(String scraperPodName, String clusterName) {
        LabelSelector uoSelector = kubeClient().getDeploymentSelectors(KafkaResources.entityOperatorDeploymentName(clusterName));
        return collectMetricsFromPods(scraperPodName, uoSelector, Constants.USER_OPERATOR_METRICS_PORT, "/metrics");
    }

    public static HashMap<String, String> collectTopicOperatorPodMetrics(String scraperPodName, String clusterName) {
        LabelSelector toSelector = kubeClient().getDeploymentSelectors(KafkaResources.entityOperatorDeploymentName(clusterName));
        return collectMetricsFromPods(scraperPodName, toSelector, Constants.TOPIC_OPERATOR_METRICS_PORT, "/metrics");
    }

    public static HashMap<String, String> collectClusterOperatorPodMetrics(String scraperPodName) {
        LabelSelector coSelector = kubeClient().getDeploymentSelectors(ResourceManager.getCoDeploymentName());
        return collectMetricsFromPods(scraperPodName, coSelector, Constants.CLUSTER_OPERATOR_METRICS_PORT, "/metrics");
    }

    public static HashMap<String, String> collectKafkaBridgePodMetrics(String scraperPodName, String clusterName) {
        LabelSelector coSelector = kubeClient().getDeploymentSelectors(KafkaBridgeResources.deploymentName(clusterName));
        return collectMetricsFromPods(scraperPodName, coSelector, Constants.KAFKA_BRIDGE_METRICS_PORT, "/metrics");
    }

    /**
     * Parse out specific metric from whole metrics file
     * @param pattern regex pattern for specific metric
     * @param data all metrics data
     * @return list of parsed values
     */
    public static ArrayList<Double> collectSpecificMetric(Pattern pattern, HashMap<String, String> data) {
        ArrayList<Double> values = new ArrayList<>();

        for (Map.Entry<String, String> entry : data.entrySet()) {
            Matcher t = pattern.matcher(entry.getValue());
            if (t.find()) {
                values.add(Double.parseDouble(t.group(1)));
            }
        }
        return values;
    }

    /**
     * Collect metrics from all pods with specific selector
     * @param scraperPodName name of pod from where the metrics will be scraped
     * @param labelSelector pod selector
     * @param port port where metrics are exposed
     * @return map with metrics {podName, metrics}
     */
    public static HashMap<String, String> collectMetricsFromPods(String scraperPodName, LabelSelector labelSelector, int port) {
        return collectMetricsFromPods(scraperPodName, labelSelector, port, "");
    }

    /**
     * Collect metrics from all pods with specific selector
     * @param scraperPodName name of pod from where the metrics will be scraped
     * @param labelSelector pod selector
     * @param port port where metrics are exposed
     * @param metricsPath additional path where metrics are available
     * @return map with metrics {podName, metrics}
     */
    public static HashMap<String, String> collectMetricsFromPods(String scraperPodName, LabelSelector labelSelector, int port, String metricsPath) {
        HashMap<String, String> map = new HashMap<>();
        kubeClient().listPods(labelSelector).forEach(p -> {
            try {
                map.put(p.getMetadata().getName(), collectMetrics(scraperPodName, p.getStatus().getPodIP(), port, metricsPath));
            } catch (InterruptedException | ExecutionException | IOException e) {
                throw new RuntimeException(e);
            }
        });
        return  map;
    }
}
