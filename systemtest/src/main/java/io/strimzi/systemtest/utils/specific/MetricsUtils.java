/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
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

    public static String getExporterRunScript(String podName, String namespace) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("cat");
        command.add("/tmp/run.sh");
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", namespace, "--"));
        executableCommand.addAll(command);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the Pod
        int ret = exec.execute(null, executableCommand, 20_000);

        synchronized (LOCK) {
            LOGGER.info("Metrics collection for Pod: {}/{} return code - {}", namespace, podName, ret);
        }

        assertThat("Collected metrics should not be empty", exec.out(), not(emptyString()));
        return exec.out();
    }

    public static MetricsCollector setupCOMetricsCollectorInNamespace(String coName, String coNamespace, String coScraperName) {
        LabelSelector scraperDeploymentPodLabel = new LabelSelector(null, Map.of(TestConstants.APP_POD_LABEL, coScraperName));
        String coScraperPodName = ResourceManager.kubeClient().listPods(coNamespace, scraperDeploymentPodLabel).get(0).getMetadata().getName();

        return new MetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(coNamespace)
            .withComponentType(ComponentType.ClusterOperator)
            .withComponentName(coName)
            .build();
    }

    public static void assertCoMetricResourceNotNull(MetricsCollector collector, String metric, String kind) {
        assertMetricResourceNotNull(collector, metric, kind);
    }

    public static void assertMetricResourceNotNull(MetricsCollector collector, String metric, String kind) {
        String metrics = metric + "\\{kind=\"" + kind + "\",.*}";
        assertMetricValueNotNull(collector, metrics);
    }

    public static void assertCoMetricResourceStateNotExists(MetricsCollector collector, String kind, String name, String namespace) {
        String metric = "strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",resource_namespace=\"" + namespace + "\",}";
        ArrayList<Double> values = createPatternAndCollectWithoutWait(collector, metric);
        assertThat(values.isEmpty(), is(true));
    }

    public static void assertCoMetricResourceState(MetricsCollector collector, String kind, String name, String namespace, int value, String reason) {
        assertMetricResourceState(collector, kind, name, namespace, value, reason);
    }

    public static void assertMetricResourceState(MetricsCollector collector, String kind, String name, String namespace, int value, String reason) {
        String metric = "strimzi_resource_state\\{kind=\"" + kind + "\",name=\"" + name + "\",reason=\"" + reason + ".*\",resource_namespace=\"" + namespace + "\",}";
        assertMetricValue(collector, metric, value);
    }

    public static void assertCoMetricResources(MetricsCollector collector, String kind, String namespace, int value) {
        assertMetricResources(collector, kind, namespace, value);
    }

    public static void assertMetricResources(MetricsCollector collector, String kind, String namespace, int value) {
        assertMetricValue(collector, getResourceMetricPattern(kind, namespace), value);
    }

    public static void assertCoMetricResourcesNullOrZero(MetricsCollector collector, String kind, String namespace) {
        Pattern pattern = Pattern.compile(getResourceMetricPattern(kind, namespace));
        if (!collector.collectSpecificMetric(pattern).isEmpty()) {
            assertThat(String.format("metric %s doesn't contain 0 value!", pattern), createPatternAndCollectWithoutWait(collector, pattern.toString()).stream().mapToDouble(i -> i).sum(), is(0.0));
        }
    }

    public static String getResourceMetricPattern(String kind, String namespace) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",";
        metric += namespace == null ? ".*}" : "namespace=\"" + namespace + "\",.*}";
        return metric;
    }

    public static void assertMetricResourcesHigherThanOrEqualTo(MetricsCollector collector, String kind, int expectedValue) {
        Predicate<Double> higherOrEqualToExpected = actual -> actual >= expectedValue;
        String metricConditionDescription = "higher or equal to expected value: (%s)".formatted(expectedValue);
        assertMetricResourcesIs(collector, kind, higherOrEqualToExpected, metricConditionDescription);
    }

    public static void assertMetricResourcesLowerThanOrEqualTo(MetricsCollector collector, String kind, int expectedValue) {
        Predicate<Double> lowerOrEqualToExpected = actual -> actual <= expectedValue;
        String metricConditionDescription = "lower or equal to expected value: (%s)".formatted(expectedValue);
        assertMetricResourcesIs(collector, kind, lowerOrEqualToExpected, metricConditionDescription);
    }

    public static void assertMetricResourcesIs(MetricsCollector collector, String kind, Predicate<Double> predicate, String message) {
        String metric = "strimzi_resources\\{kind=\"" + kind + "\",.*}";
        TestUtils.waitFor("metric " + metric + "is " + message, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT_SHORT, () -> {
            collector.collectMetricsFromPods();
            ArrayList<Double> values = createPatternAndCollect(collector, metric);
            double actualValue = values.stream().mapToDouble(i -> i).sum();
            return predicate.test(actualValue);
        });
    }

    public static void assertMetricValueNotNull(MetricsCollector collector, String metric) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' doesn't exist", metric), actualValue, notNullValue());
    }

    public static void assertMetricValueNullOrZero(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        if (!collector.collectSpecificMetric(pattern).isEmpty()) {
            assertThat(String.format("metric %s doesn't contain 0 value!", pattern), createPatternAndCollectWithoutWait(collector, pattern.toString()).stream().mapToDouble(i -> i).sum(), is(0.0));
        }
    }

    public static void assertMetricValue(MetricsCollector collector, String metric, int expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue, is((double) expectedValue));
    }

    public static void assertMetricValueCount(MetricsCollector collector, String metric, long expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", actualValue, expectedValue, metric), actualValue, is((double) expectedValue));
    }

    public static void assertMetricCountHigherThan(MetricsCollector collector, String metric, long expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).count();
        assertThat(String.format("metric '%s' actual value %s not is higher than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    public static void assertMetricValueHigherThan(MetricsCollector collector, String metric, int expectedValue) {
        ArrayList<Double> values = createPatternAndCollect(collector, metric);
        double actualValue = values.stream().mapToDouble(i -> i).sum();
        assertThat(String.format("metric '%s' actual value %s is different than expected %s", metric, actualValue, expectedValue), actualValue > expectedValue);
    }

    private static ArrayList<Double> createPatternAndCollect(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.waitForSpecificMetricAndCollect(pattern);
    }

    private static ArrayList<Double> createPatternAndCollectWithoutWait(MetricsCollector collector, String metric) {
        Pattern pattern = Pattern.compile(metric + " ([\\d.][^\\n]+)", Pattern.CASE_INSENSITIVE);
        return collector.collectSpecificMetric(pattern);
    }
}
