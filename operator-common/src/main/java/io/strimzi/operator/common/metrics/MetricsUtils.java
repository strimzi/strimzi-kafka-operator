/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.Optional;
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
     * Checks if the given meter matches the given metric name
     *
     * @param meter         Meter to check
     * @param metricName    Expected metric name to check
     * @return  True if the meter matches the metric name, false otherwise
     */
    protected static boolean isMatchingMetricName(Meter meter, String metricName) {
        return meter.getId().getName().equals(metricName);
    }

    /**
     * Checks if the given meter matches the given metric tags
     *
     * @param meterTags     Tags of the meter to check
     * @param expectedTags  Expected tags to check
     * @return  True if the meter matches the metric tags, false otherwise
     */
    protected static boolean isMatchingMetricTags(Set<Tag> meterTags, Set<Tag> expectedTags) {
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
            if (meterTag.getKey().equals(expectedTag.getKey())
                    && meterTag.getValue().equals(expectedTag.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Constructing the all tags for the metrics internally from the static metrics methods
     *
     * @param namespace         Namespace of the resources being reconciled
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param optionalTags      Optional tags to be used to construct the metric key
     * @return  All tags for the metric combined
     */
    public static Tags getAllMetricTags(String namespace, String kind, Optional<String> selectorLabels, Tag... optionalTags) {
        Tags requiredTags = getRequiredMetricTags(namespace, kind, selectorLabels);
        if (optionalTags != null && optionalTags.length > 0) {
            requiredTags = requiredTags.and(optionalTags);
        }
        return requiredTags;
    }

    /**
     * Constructing the tags for the metrics internally from the static metrics methods
     *
     * @param namespace         Namespace of the resources being reconciled
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @return  Tags
     */
    protected static Tags getRequiredMetricTags(String namespace, String kind, Optional<String> selectorLabels) {
        Tags tags = Tags.of(
                Tag.of("kind", kind),
                Tag.of("namespace", namespace.equals("*") ? "" : namespace));
        if (selectorLabels.isPresent()) {
            tags = tags.and(Tag.of("selector", selectorLabels.get()));
        }
        return tags;
    }
}
