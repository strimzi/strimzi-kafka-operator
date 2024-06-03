/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.TestConstants.GLOBAL_POLL_INTERVAL;
import static io.strimzi.systemtest.TestConstants.GLOBAL_TIMEOUT;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class MetricsCollector {

    private static final Logger LOGGER = LogManager.getLogger(MetricsCollector.class);

    private static final Object LOCK = new Object();

    protected String namespaceName;
    protected String scraperPodName;
    protected ComponentType componentType;
    protected String componentName;
    protected int metricsPort;
    protected String metricsPath;
    protected LabelSelector componentLabelSelector;
    protected Map<String, String> collectedData;

    public static class Builder {
        private String namespaceName;
        private String scraperPodName;
        private ComponentType componentType;
        private String componentName;
        private int metricsPort;
        private String metricsPath = "/metrics";

        public Builder withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public Builder withScraperPodName(String scraperPodName) {
            this.scraperPodName = scraperPodName;
            return this;
        }

        public Builder withComponentType(ComponentType componentType) {
            this.componentType = componentType;
            return this;
        }

        public Builder withComponentName(String componentName) {
            this.componentName = componentName;
            return this;
        }

        public Builder withMetricsPort(int metricsPort) {
            this.metricsPort = metricsPort;
            return this;
        }

        public Builder withMetricsPath(String metricsPath) {
            this.metricsPath = metricsPath;
            return this;
        }

        public MetricsCollector build() {
            return new MetricsCollector(this);
        }
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public String getScraperPodName() {
        return scraperPodName;
    }

    public ComponentType getComponentType() {
        return componentType;
    }

    public String getComponentName() {
        return componentName;
    }

    public String getMetricsPath() {
        return metricsPath;
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public Map<String, String> getCollectedData() {
        return collectedData;
    }

    protected MetricsCollector.Builder newBuilder() {
        return new MetricsCollector.Builder();
    }

    protected MetricsCollector.Builder updateBuilder(MetricsCollector.Builder builder) {
        return builder
            .withNamespaceName(getNamespaceName())
            .withComponentName(getComponentName())
            .withComponentType(getComponentType())
            .withScraperPodName(getScraperPodName());
    }

    public MetricsCollector.Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    protected MetricsCollector(Builder builder) {
        if (builder.namespaceName == null || builder.namespaceName.isEmpty()) builder.namespaceName = kubeClient().getNamespace();
        if (builder.scraperPodName == null || builder.scraperPodName.isEmpty()) throw new InvalidParameterException("Scraper Pod name is not set");
        if (builder.componentType == null) throw new InvalidParameterException("Component type is not set");
        if (builder.componentName == null || builder.componentName.isEmpty()) {
            if (!builder.componentType.equals(ComponentType.ClusterOperator)) {
                throw new InvalidParameterException("Component name is not set");
            }
        }

        componentType = builder.componentType;

        if (builder.metricsPort <= 0) builder.metricsPort = getDefaultMetricsPortForComponent();

        namespaceName = builder.namespaceName;
        scraperPodName = builder.scraperPodName;
        metricsPort = builder.metricsPort;
        metricsPath = builder.metricsPath;
        componentName = builder.componentName;
        componentLabelSelector = getLabelSelectorForResource();
    }

    private LabelSelector getLabelSelectorForResource() {
        switch (this.componentType) {
            case Kafka:
                return KafkaResource.getLabelSelector(componentName, StrimziPodSetResource.getBrokerComponentName(componentName));
            case Zookeeper:
                return KafkaResource.getLabelSelector(componentName, KafkaResources.zookeeperComponentName(componentName));
            case KafkaConnect:
                return KafkaConnectResource.getLabelSelector(componentName, KafkaConnectResources.componentName(componentName));
            case KafkaExporter:
                return kubeClient().getDeploymentSelectors(namespaceName, KafkaExporterResources.componentName(componentName));
            case KafkaMirrorMaker2:
                return KafkaMirrorMaker2Resource.getLabelSelector(componentName, KafkaMirrorMaker2Resources.componentName(componentName));
            case UserOperator:
            case TopicOperator:
                return kubeClient().getDeploymentSelectors(namespaceName, KafkaResources.entityOperatorDeploymentName(componentName));
            case ClusterOperator:
                return kubeClient().getDeploymentSelectors(namespaceName, componentName);
            case KafkaBridge:
                return kubeClient().getDeploymentSelectors(namespaceName, KafkaBridgeResources.componentName(componentName));
            default:
                return new LabelSelector();
        }
    }

    private int getDefaultMetricsPortForComponent() {
        switch (this.componentType) {
            case UserOperator:
                return TestConstants.USER_OPERATOR_METRICS_PORT;
            case TopicOperator:
                return TestConstants.TOPIC_OPERATOR_METRICS_PORT;
            case ClusterOperator:
                return TestConstants.CLUSTER_OPERATOR_METRICS_PORT;
            case KafkaBridge:
                return TestConstants.KAFKA_BRIDGE_METRICS_PORT;
            default:
                return TestConstants.COMPONENTS_METRICS_PORT;
        }
    }

    /**
     * Parse out specific metric from whole metrics file
     * @param pattern regex pattern for specific metric
     * @return list of parsed values
     */
    public final ArrayList<Double> collectSpecificMetric(Pattern pattern) {
        ArrayList<Double> values = new ArrayList<>();

        if (collectedData != null && !collectedData.isEmpty()) {
            for (Map.Entry<String, String> entry : collectedData.entrySet()) {
                Matcher t = pattern.matcher(entry.getValue());
                if (t.find()) {
                    values.add(Double.parseDouble(t.group(1)));
                }
            }
        }

        return values;
    }

    /**
     * Parses out a specific metric with varying labels from the entire metrics data.
     * @param metricName The name of the metric to collect.
     * @return A map where each key represents the unique labels and the value is the corresponding metric value.
     */
    public final Map<String, Double> collectMetricWithLabels(String metricName) {
        // This pattern will match the metric name and capture the labels and value.
        Pattern pattern = Pattern.compile(metricName + "\\{([^}]+)\\}\\s(\\d+(?:\\.\\d+)?(?:E-?\\d+)?)");
        Map<String, Double> valuesWithLabels = new HashMap<>();

        if (collectedData != null && !collectedData.isEmpty()) {
            for (String dataLine : collectedData.values()) {
                Matcher matcher = pattern.matcher(dataLine);
                while (matcher.find()) {
                    // Construct the key from the metric name and labels.
                    String key = metricName + "{" + matcher.group(1) + "}";
                    Double value = Double.parseDouble(matcher.group(2));
                    valuesWithLabels.put(key, value);
                }
            }
        }

        return valuesWithLabels;
    }

    /**
     * Method checks already collected metrics data for Pattern containing desired metric
     * @param pattern Pattern of metric which is desired
     *
     * @return ArrayList of values collected from the metrics
     */
    public final synchronized ArrayList<Double> waitForSpecificMetricAndCollect(Pattern pattern) {
        ArrayList<Double> values = collectSpecificMetric(pattern);

        if (values.isEmpty()) {
            TestUtils.waitFor(String.format("metrics contain pattern: %s", pattern.toString()), TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
                this.collectMetricsFromPods();
                LOGGER.debug("Collected data: {}", collectedData);
                ArrayList<Double> vals = this.collectSpecificMetric(pattern);

                if (!vals.isEmpty()) {
                    values.addAll(vals);
                    return true;
                }

                return false;
            });
        }

        return values;
    }

    /**
     * Collect metrics from specific pod
     * @return collected metrics
     */
    private String collectMetrics(String metricsPodIp, String podName) throws InterruptedException, ExecutionException, IOException {
        List<String> executableCommand = Arrays.asList(cmdKubeClient(namespaceName).toString(), "exec", scraperPodName,
            "-n", namespaceName,
            "--", "curl", metricsPodIp + ":" + metricsPort + metricsPath);

        LOGGER.debug("Executing command:{} for scrape the metrics", executableCommand);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the pod
        int ret = exec.execute(null, executableCommand, 20_000);

        LOGGER.log(ResourceManager.getInstance().determineLogLevel(), "Metrics collection for Pod: {}/{}({}) from Pod: {}/{} finished with return code: {}", namespaceName, podName, metricsPodIp, namespaceName, scraperPodName, ret);
        return exec.out();
    }

    /**
     * Collect metrics from all Pods with specific selector with wait
     */
    @SuppressWarnings("unchecked")
    public final void collectMetricsFromPods() {
        Map<String, String>[] metricsData = (Map<String, String>[]) new HashMap[1];
        TestUtils.waitFor("metrics to contain data", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT,
            () -> {
                metricsData[0] = collectMetricsFromPodsWithoutWait();

                // KafkaExporter metrics should be non-empty
                if (!(metricsData[0].size() > 0)) {
                    return false;
                }

                for (Map.Entry<String, String> item : metricsData[0].entrySet()) {
                    if (item.getValue().isEmpty()) {
                        return false;
                    }
                }
                return true;
            });

        collectedData = metricsData[0];
    }

    public final Map<String, String> collectMetricsFromPodsWithoutWait() {
        Map<String, String> map = new HashMap<>();
        kubeClient(namespaceName).listPods(namespaceName, componentLabelSelector).forEach(p -> {
            try {
                final String podName = p.getMetadata().getName();
                String podIP = p.getStatus().getPodIP();

                if (Environment.isIpv6Family()) {
                    // for curl command we need to add '[' and ']' to make it work
                    // f.e. http://[fd00:10:244::1d]:9404 this would work but http://fd00:10:244::1d:9404 will not
                    podIP = "[" + podIP + "]";
                }

                map.put(podName, collectMetrics(podIP, podName));
            } catch (InterruptedException | ExecutionException | IOException e) {
                throw new RuntimeException(e);
            }
        });
        return  map;
    }
}
