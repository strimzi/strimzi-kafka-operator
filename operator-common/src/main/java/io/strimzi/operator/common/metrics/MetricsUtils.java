/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class for metrics
 */
public class MetricsUtils {
    /**
     * Private constructor to prevent instantiation
     */
    private MetricsUtils() {
        // Intentionally left blank
    }

    /**
     * Tag representing any namespace
     */
    protected static final Tag TAG_ANY_NAMESPACE = Tag.of("namespace", ConfigParameter.ANY_NAMESPACE);

    /**
     * Checks if the given meter matches the given metric name
     *
     * @param meter         Meter to check
     * @param metricName    Expected metric name to check
     * @return  True if the meter matches the metric name, false otherwise
     */
    public static boolean isMatchingMetricName(Meter meter, String metricName) {
        return meter.getId().getName().equals(metricName);
    }

    /**
     * Checks if the given meter matches the given metric tags
     *
     * @param meterTags     Tags of the meter to check
     * @param expectedTags  Expected tags to check
     * @return  True if the meter matches the metric tags, false otherwise
     */
    public static boolean isMatchingMetricTags(Set<Tag> meterTags, Set<Tag> expectedTags) {
        if (!meterTags.containsAll(expectedTags)) {
            return false;
        }

        return expectedTags.stream().allMatch(tag -> isMatchingTag(meterTags, tag));
    }

    /**
     * Checks if the meter tags is matches any expected tag
     *
     * @param meterTags     Tags of the meter to check
     * @param expectedTag   Expected tag to check
     * @return  True if the meter matches the metric tag, false otherwise
     */
    protected static boolean isMatchingTag(Set<Tag> meterTags, Tag expectedTag) {
        for (Tag meterTag : meterTags) {
            if (expectedTag.getKey().equals(TAG_ANY_NAMESPACE.getKey())
                    && expectedTag.getValue().equals(TAG_ANY_NAMESPACE.getValue())) {
                return true;
            }

            if (meterTag.getKey().equals(expectedTag.getKey())
                    && meterTag.getValue().equals(expectedTag.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Constructing the metric key
     *
     * @param clusterName   Name of the cluster
     * @param namespace     Namespace of the resources being reconciled
     * @param kind          Kind of the resources for which these metrics apply
     * @return metric key
     */
    public static String getMetricKey(String clusterName, String namespace, String kind) {
        final List<String> metricKeys = new ArrayList<>();
        if (clusterName != null) {
            metricKeys.add(clusterName);
        }
        metricKeys.add(namespace);
        metricKeys.add(kind);

        return Strings.join(metricKeys, '/');
    }

    /**
     * Constructing the tags for the metrics internally from the static metrics methods
     *
     * @param clusterName       Name of the cluster
     * @param namespace         Namespace of the resources being reconciled
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @return  Tags
     */
    public static Tags getMetricTags(String clusterName, String namespace, String kind, Labels selectorLabels) {
        return Tags.of(
                Tag.of("kind", kind),
                Tag.of("namespace", namespace.equals("*") ? "" : namespace),
                Tag.of("cluster_name", clusterName == null ? "" : clusterName),
                Tag.of("selector", selectorLabels != null ? selectorLabels.toSelectorString() : ""));
    }
}
