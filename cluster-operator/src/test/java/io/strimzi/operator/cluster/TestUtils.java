/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.operator.cluster.model.ModelUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class TestUtils {
    public static JmxPrometheusExporterMetrics getJmxPrometheusExporterMetrics(String key, String name) {
        return new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withName(name)
                            .withKey(key)
                            .withOptional(true)
                            .build())
                .endValueFrom()
                .build();
    }

    public static ConfigMap getJmxMetricsCm(String data, String metricsCMName, String metricsConfigYaml) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(metricsCMName)
                .endMetadata()
                .withData(singletonMap(metricsConfigYaml, data))
                .build();
    }

    /**
     * Gets the given container's environment as a Map. This makes it easier to verify the environment variables in
     * unit tests.
     *
     * @param container The container to retrieve the EnvVars from
     *
     * @return A map of the environment variables of the given container. The Environmental variable values indexed by
     * their names
     */
    public static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue,
                        // On duplicates, last-in wins
                        (u, v) -> v));
    }

    /**
     * Class used to compare collection of lines (current against expected)
     * It can be used, for example, to compare configurations made by key=value pair (properties)
     * to check that the current one is equivalent to the expected (not taking declaration order into account)
     */
    public static class IsEquivalent extends TypeSafeMatcher<String> {
        private final List<String> expectedLines;

        public IsEquivalent(String expectedConfig) {
            super();
            this.expectedLines = ModelUtils.getLinesWithoutCommentsAndEmptyLines(expectedConfig);
        }

        public IsEquivalent(List<String> expectedLines) {
            super();
            this.expectedLines = expectedLines;
        }

        @Override
        protected boolean matchesSafely(String config) {
            List<String> actualLines = ModelUtils.getLinesWithoutCommentsAndEmptyLines(config);

            return expectedLines.containsAll(actualLines) && actualLines.containsAll(expectedLines);
        }

        private String getLinesAsString(Collection<String> configLines)   {
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);

            for (String line : configLines) {
                writer.println(line);
            }

            return stringWriter.toString();
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(getLinesAsString(new TreeSet<>(expectedLines)));
        }

        @Override
        protected void describeMismatchSafely(String item, Description mismatchDescription) {
            printDiff(item, mismatchDescription);
        }

        private void printDiff(String item, Description mismatchDescription)    {
            List<String> actualLines = ModelUtils.getLinesWithoutCommentsAndEmptyLines(item);
            List<String> actualLinesDiff = new ArrayList<>(actualLines);
            actualLinesDiff.removeAll(expectedLines);
            List<String> expectedLinesDiff = new ArrayList<>(expectedLines);
            expectedLinesDiff.removeAll(actualLines);

            mismatchDescription
                    .appendText(" was: \n")
                    .appendText(getLinesAsString(new TreeSet<>(ModelUtils.getLinesWithoutCommentsAndEmptyLines(item))))
                    .appendText("\n\n")
                    .appendText(" wrong lines in expected:\n")
                    .appendText(getLinesAsString(expectedLinesDiff))
                    .appendText("\n\n")
                    .appendText(" Wrong lines in actual:\n")
                    .appendText(getLinesAsString(actualLinesDiff))
                    .appendText("\n\nOriginal value: \n")
                    .appendText(item)
                    .appendText("\n\n");
        }

        public static Matcher<String> isEquivalent(String expectedConfig) {
            return new IsEquivalent(expectedConfig);
        }

        public static Matcher<String> isEquivalent(String... expectedLines) {
            return new IsEquivalent(asList(expectedLines));
        }
    }
}
