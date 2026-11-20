/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.storage.TestStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;


/**
 * Extends the {@link BasePerformanceReporter} to report on performance metrics specific to the Topic Operator.
 * <p>
 * This implementation customizes the path resolution for performance logs by incorporating Topic Operator-specific
 * attributes such as maximum batch size, maximum linger milliseconds, and whether client instances are enabled.
 */
public class TopicOperatorPerformanceReporter extends BasePerformanceReporter {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorPerformanceReporter.class);
    private static final String REPORT_DIRECTORY = "topic-operator";

    /** Creates a builder for a Topic Operator capacity report. */
    public TopicCapacityBuilder capacity() {
        return new TopicCapacityBuilder();
    }

    /** Creates a builder for a Topic Operator scalability report. */
    public TopicScalabilityBuilder scalability() {
        return new TopicScalabilityBuilder();
    }

    /** Builder for Topic Operator capacity report attributes. */
    public final class TopicCapacityBuilder {
        private String maxQueueSize;
        private String maxBatchSize;
        private String maxBatchLingerMs;
        private int successfulCreations;
        private Map<Long, Map<String, List<Double>>> metricsHistory;

        public TopicCapacityBuilder maxQueueSize(String value) {
            maxQueueSize = value;
            return this;
        }
        public TopicCapacityBuilder maxBatchSize(String value) {
            maxBatchSize = value;
            return this;
        }
        public TopicCapacityBuilder maxBatchLingerMs(String value) {
            maxBatchLingerMs = value;
            return this;
        }
        public TopicCapacityBuilder successfulCreations(int value) {
            successfulCreations = value;
            return this;
        }
        public TopicCapacityBuilder metricsHistory(Map<Long, Map<String, List<Double>>> value) {
            metricsHistory = value;
            return this;
        }
        public void log(TestStorage storage) throws IOException {
            TopicOperatorPerformanceReporter.this.logPerformanceData(
                storage,
                attributes(),
                REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_CAPACITY_USE_CASE,
                performanceTimestamp(),
                performanceBaseDirectory()
            );
        }
        Map<String, Object> attributes() {
            Map<String, Object> attributes = new LinkedHashMap<>();
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE, maxQueueSize);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, maxBatchSize);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, maxBatchLingerMs);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_OUT_SUCCESSFUL_KAFKA_TOPICS_CREATED, successfulCreations);
            attributes.put(PerformanceConstants.METRICS_HISTORY, metricsHistory);
            return attributes;
        }
    }

    /** Builder for Topic Operator scalability report attributes. */
    public final class TopicScalabilityBuilder {
        private int maxQueueSize;
        private int maxBatchSize;
        private int numberOfTopics;
        private int numberOfEvents;
        private int maxBatchLingerMs;
        private String processType;
        private long reconciliationIntervalMs;
        public TopicScalabilityBuilder maxQueueSize(int value) {
            maxQueueSize = value;
            return this;
        }
        public TopicScalabilityBuilder maxBatchSize(int value) {
            maxBatchSize = value;
            return this;
        }
        public TopicScalabilityBuilder numberOfTopics(int value) {
            numberOfTopics = value;
            return this;
        }
        public TopicScalabilityBuilder numberOfEvents(int value) {
            numberOfEvents = value;
            return this;
        }
        public TopicScalabilityBuilder maxBatchLingerMs(int value) {
            maxBatchLingerMs = value;
            return this;
        }
        public TopicScalabilityBuilder processType(String value) {
            processType = value;
            return this;
        }
        public TopicScalabilityBuilder reconciliationIntervalMs(long value) {
            reconciliationIntervalMs = value;
            return this;
        }
        public void log(TestStorage storage) throws IOException {
            TopicOperatorPerformanceReporter.this.logPerformanceData(
                storage,
                attributes(),
                REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE,
                performanceTimestamp(),
                performanceBaseDirectory()
            );
        }
        Map<String, Object> attributes() {
            Map<String, Object> attributes = new LinkedHashMap<>();
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE, maxQueueSize);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, maxBatchSize);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS, numberOfTopics);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_EVENTS, numberOfEvents);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, maxBatchLingerMs);
            attributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_PROCESS_TYPE, processType);
            attributes.put(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL, reconciliationIntervalMs);
            return attributes;
        }
    }

    @Override
    protected Path resolveComponentUseCasePathDir(Path performanceLogDir, String useCaseName, Map<String, Object> performanceAttributes) {
        final String maxBatchSize = performanceAttributes.getOrDefault(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, "").toString();
        final String maxBatchLingerMs = performanceAttributes.getOrDefault(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, "").toString();
        final String numberOfTopics = performanceAttributes.getOrDefault(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS, 0).toString();
        final boolean clientsEnabled = !performanceAttributes.getOrDefault(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_CLIENT_INSTANCES, "0").equals("0");

        final StringBuilder dirPathBuilder = new StringBuilder();
        dirPathBuilder.append(useCaseName)
            .append("/max-batch-size-").append(maxBatchSize)
            .append("-max-linger-time-").append(maxBatchLingerMs)
            .append("-with-clients-").append(clientsEnabled);

        if (!numberOfTopics.equals("0")) {
            dirPathBuilder.append("-number-of-topics-").append(numberOfTopics);
        }

        // Defines the strategy for Kafka topic processing, either concurrently by topic or by batch.
        String processType = null;

        if (useCaseName.equals(PerformanceConstants.TOPIC_OPERATOR_FIXED_SIZE_OF_EVENTS_USE_CASE)) {
            processType = performanceAttributes.get(PerformanceConstants.TOPIC_OPERATOR_IN_PROCESS_TYPE).toString().toLowerCase(Locale.ROOT);


            // it would be either "topic-concurrent" or "batch-concurrent"
            dirPathBuilder.append("-process-type-").append(processType);
        }

        final Path topicOperatorUseCasePathDir = performanceLogDir.resolve(dirPathBuilder.toString());

        LOGGER.info("Resolved performance log directory: {} for use case '{}'. Max batch size: {}, Max linger time: {}, " +
                "Clients enabled: {}, Number of topics: {}, Process type: {}",
            topicOperatorUseCasePathDir, useCaseName, maxBatchSize, maxBatchLingerMs, clientsEnabled,
            numberOfTopics.equals("0") ? "not included" : numberOfTopics, processType == null ? "not included" : processType);

        return topicOperatorUseCasePathDir;
    }
}
