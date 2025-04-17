/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.metrics.Counter;
import io.skodjob.testframe.metrics.Gauge;
import io.skodjob.testframe.metrics.Histogram;
import io.skodjob.testframe.metrics.Metric;
import io.skodjob.testframe.metrics.Summary;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 *  Provides auxiliary methods for Scraper Pod, which reaches KafkaConnect API in the Kubernetes cluster.
 */
public class MetricsUtils {
    private static final Logger LOGGER = LogManager.getLogger(MetricsUtils.class);
    private static final Object LOCK = new Object();

    private MetricsUtils() { }

    public static String getExporterRunScript(String namespaceName, String podName) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("cat");
        command.add("/tmp/run.sh");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", namespaceName, "--"));
        executableCommand.addAll(command);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the Pod
        int ret = exec.execute(null, executableCommand, 20_000);

        synchronized (LOCK) {
            LOGGER.info("Metrics collection for Pod: {}/{} return code - {}", namespaceName, podName, ret);
        }

        assertThat("Collected metrics should not be empty", exec.out(), not(emptyString()));
        return exec.out();
    }

    public static BaseMetricsCollector setupCOMetricsCollectorInNamespace(String coNamespace, String coName, String coScraperName) {
        LabelSelector scraperDeploymentPodLabel = new LabelSelector(null, Map.of(TestConstants.APP_POD_LABEL, coScraperName));
        String coScraperPodName = kubeClient().listPods(coNamespace, scraperDeploymentPodLabel).get(0).getMetadata().getName();

        return new BaseMetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(coNamespace)
            .withComponent(ClusterOperatorMetricsComponent.create(coNamespace, coName))
            .build();
    }

    public static void assertCoMetricResourceNotNull(BaseMetricsCollector collector, String metric, String kind) {
        assertMetricResourceNotNull(collector, metric, kind);
    }

    public static void assertMetricResourceNotNull(BaseMetricsCollector collector, String metric, String kind) {
        String metrics = metric + "\\{kind=\"" + kind + "\",.*}";
        assertMetricValueNotNull(collector, metrics);
    }

    public static void assertCoMetricResourceStateNotExists(String namespaceName, String kind, String name, BaseMetricsCollector collector) {
        String metric = "strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",resource_namespace=\"" + namespaceName + "\",}";
        List<Double> values = createPatternAndCollectWithoutWait(collector, metric);
        assertThat(values.isEmpty(), is(true));
    }

    public static void assertCoMetricResourceState(String namespaceName, String kind, String name, BaseMetricsCollector collector, double value, String reason) {
        assertMetricResourceState(namespaceName, kind, name, collector, value, reason);
    }

    public static void assertMetricResourceState(String namespaceName, String kind, String name, BaseMetricsCollector collector, double value, String reason) {
        String metric = "strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",reason=\"" + reason + ".*\",resource_namespace=\"" + namespaceName + "\",}";
        assertMetricValue(collector, metric, value);
    }

    public static void assertCoMetricResources(String namespaceName, String kind, BaseMetricsCollector collector, double value) {
        assertMetricResources(namespaceName, kind, collector, value);
    }

    public static void assertMetricResources(String namespaceName, String kind, BaseMetricsCollector collector, double value) {
        assertMetricValue(collector, getResourceMetricPattern(namespaceName, kind), value);
    }

    public static void assertCoMetricResourcesNullOrZero(String namespaceName, String kind, BaseMetricsCollector collector) {
        Pattern pattern = Pattern.compile(getResourceMetricPattern(namespaceName, kind));
        if (!collector.collectSpecificMetric(pattern).isEmpty()) {
            assertThat(String.format("metric %s doesn't contain 0 value!", pattern), createPatternAndCollectWithoutWait(collector, pattern.toString()).stream().mapToDouble(i -> i).sum(), is(0.0));
        }
    }

    public static String getResourceMetricPattern(String namespaceName, String kind) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",";
        metric += namespaceName == null ? ".*}" : "namespace=\"" + namespaceName + "\",.*}";
        return metric;
    }

    public static void assertMetricResourcesHigherThanOrEqualTo(BaseMetricsCollector collector, String kind, int expectedValue) {
        Predicate<Double> higherOrEqualToExpected = actual -> actual >= expectedValue;
        String metricConditionDescription = "higher or equal to expected value: (%s)".formatted(expectedValue);
        assertMetricResourcesIs(collector, kind, higherOrEqualToExpected, metricConditionDescription);
    }

    public static void assertMetricResourcesLowerThanOrEqualTo(BaseMetricsCollector collector, String kind, int expectedValue) {
        Predicate<Double> lowerOrEqualToExpected = actual -> actual <= expectedValue;
        String metricConditionDescription = "lower or equal to expected value: (%s)".formatted(expectedValue);
        assertMetricResourcesIs(collector, kind, lowerOrEqualToExpected, metricConditionDescription);
    }

    public static void assertMetricResourcesIs(BaseMetricsCollector collector, String kind, Predicate<Double> predicate, String message) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",.*}";
        TestUtils.waitFor("metric " + metric + "is " + message, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT_SHORT, () -> {
            collector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
            List<Double> values = createPatternAndCollect(collector, metric);
            double actualValue = values.stream().mapToDouble(i -> i).sum();
            return predicate.test(actualValue);
        });
    }

    public static void assertMetricValueNotNull(BaseMetricsCollector collector, String metric) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' doesn't exist", metric), actualValue, notNullValue());
    }

    public static void assertMetricValueNullOrZero(BaseMetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        if (!collector.collectSpecificMetric(pattern).isEmpty()) {
            assertThat(String.format("metric %s doesn't contain 0 value!", pattern), createPatternAndCollectWithoutWait(collector, pattern.toString()).stream().mapToDouble(i -> i).sum(), is(0.0));
        }
    }

    public static void assertMetricValue(BaseMetricsCollector collector, String metric, double expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue, is(expectedValue));
    }

    public static void assertMetricValueCount(BaseMetricsCollector collector, String metric, double expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", actualValue, expectedValue, metric), actualValue, is(expectedValue));
    }

    public static void assertMetricCountHigherThan(BaseMetricsCollector collector, String metric, double expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s is not higher than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    public static void assertMetricValueHigherThanOrEqualTo(BaseMetricsCollector collector, String metric, double expectedValue) {
        List<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is not higher than or equal to %s", metric, actualValue, expectedValue), actualValue >= expectedValue);
    }

    public static void assertContainsMetric(List<Metric> metrics, String metricName) {
        boolean containsMetric = metrics.stream().anyMatch(metric -> metric.getName().contains(metricName));
        assertThat(String.format("metric '%s' is not present in the list of metrics", metricName), containsMetric, is(true));
    }

    private static List<Double> createPatternAndCollect(BaseMetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.waitForSpecificMetricAndCollect(pattern);
    }

    private static List<Double> createPatternAndCollectWithoutWait(BaseMetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.collectSpecificMetric(pattern);
    }

    public static Double getDoubleMetricValueBasedOnType(Metric metric) {
        if (metric instanceof Gauge gauge) {
            return gauge.getValue();
        } else if (metric instanceof Histogram histogram) {
            return histogram.getSum();
        } else if (metric instanceof Counter counter) {
            return counter.getValue();
        } else if (metric instanceof Summary summary) {
            return summary.getSum();
        }

        return null;
    }
}
