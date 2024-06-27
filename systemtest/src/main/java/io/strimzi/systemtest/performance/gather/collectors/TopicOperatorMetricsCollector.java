/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.collectors;

import io.skodjob.testframe.MetricsCollector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.systemtest.performance.PerformanceConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Collects metrics related to the Topic Operator within Strimzi, facilitating the monitoring of various operational aspects.
 * This class extends {@link BaseMetricsCollector} to gather metrics specific to topic operations, reconciliations, JVM, and system performance.
 */
public class TopicOperatorMetricsCollector extends BaseMetricsCollector {

    /**
     * Constructs a {@code TopicOperatorMetricsCollector} using the builder pattern for configuration.
     *
     * @param builder the {@link MetricsCollector.Builder} used to configure the collector
     */
    protected TopicOperatorMetricsCollector(MetricsCollector.Builder builder) {
        super(builder);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- SUM OPERATION METRICS ----------------------------------------------
    // -----------------------------------------------------------------------------------------------------
    public List<Double> getAlterConfigsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.ALTER_CONFIGS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getRemoveFinalizerDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.REMOVE_FINALIZER_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getReconciliationsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getCreateTopicsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getDescribeTopicsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_TOPICS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getCreatePartitionsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.CREATE_PARTITIONS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getAddFinalizerDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.ADD_FINALIZER_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getUpdateStatusDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getDescribeConfigsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getDeleteTopicsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.DELETE_TOPICS_DURATION_SECONDS_SUM, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // --------------------------------- RECONCILIATIONS METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getReconciliationsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX, selector);
    }

    public List<Double> getReconciliationsMaxQueueSize(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_MAX_QUEUE_SIZE, selector);
    }

    public List<Double> getReconciliationsMaxBatchSize(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_MAX_BATCH_SIZE, selector);
    }

    public List<Double> getReconciliationsSuccessfulTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_SUCCESSFUL_TOTAL, selector);
    }

    public List<Double> getReconciliationsTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_TOTAL, selector);
    }

    public List<Double> getReconciliationsFailedTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_FAILED_TOTAL, selector);
    }

    public List<Double> getReconciliationsLockedTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_LOCKED_TOTAL, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- CREATE OPERATION METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getCreateTopicsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- DELETE OPERATION METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getDeleteTopicsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.DELETE_TOPICS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- UPDATE OPERATION METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getUpdateStatusDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // ------------------------------- DESCRIBE OPERATION METRICS ------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getDescribeTopicsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_TOPICS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- CONFIGS OPERATION METRICS ------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getAlterConfigsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.ALTER_CONFIGS_DURATION_SECONDS_MAX, selector);
    }

    public List<Double> getDescribeConfigsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // --------------------------------  RESOURCE METRICS ----------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getResourcesKafkaTopics(String selector) {
        return collectMetricValues(PerformanceConstants.RESOURCES, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // ---------------------------------- FINALIZERS METRICS -----------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getAddFinalizerDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.ADD_FINALIZER_DURATION_SECONDS_MAX, selector);
    }

    public List<Double> getRemoveFinalizerDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.REMOVE_FINALIZER_DURATION_SECONDS_MAX, selector);
    }

    // Example of a bucketed metric method
    public List<Double> getAlterConfigsDurationSecondsBucket(String selector, String le) {
        Pattern pattern = Pattern.compile("strimzi_alter_configs_duration_seconds_bucket\\{kind=\"" + KafkaTopic.RESOURCE_KIND + "\",namespace=\"" + this.getNamespaceName() + "\",selector=\"" + selector + "\",le=\"" + le + "\",\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    // Example for a method collecting multiple buckets at once
    public Map<String, List<Double>> getAlterConfigsDurationSecondsBuckets(String selector, List<String> les) {
        Map<String, List<Double>> buckets = new HashMap<>();
        les.forEach(le -> buckets.put(le, getAlterConfigsDurationSecondsBucket(selector, le)));
        return buckets;
    }

    // -----------------------------------------------------------------------------------------------------

    /**
     * Builder for {@link TopicOperatorMetricsCollector}, allowing for customizable configuration.
     */
    public static class Builder extends MetricsCollector.Builder {
        @Override
        public TopicOperatorMetricsCollector build() {
            return new TopicOperatorMetricsCollector(this);
        }

        // Override the builder methods to return the type of this Builder, allowing method chaining
        @Override
        public Builder withNamespaceName(String namespaceName) {
            super.withNamespaceName(namespaceName);
            return this;
        }

        @Override
        public Builder withScraperPodName(String scraperPodName) {
            super.withScraperPodName(scraperPodName);
            return this;
        }

        @Override
        public Builder withComponent(MetricsComponent component) {
            super.withComponent(component);
            return this;
        }
    }
}