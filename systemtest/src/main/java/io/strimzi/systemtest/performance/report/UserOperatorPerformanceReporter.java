/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils.LatencyMetrics;
import io.strimzi.systemtest.storage.TestStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class extends {@link BasePerformanceReporter} to implement performance reporting
 * specifically tailored for the Kafka User Operator within Strimzi. It overrides the method to resolve
 * the directory path for storing performance logs, incorporating a comprehensive set of performance
 * attributes to generate a unique and descriptive directory path for each set of test conditions.
 *
 * The class focuses on logging performance metrics related to the creation and management of Kafka users,
 * using various operational parameters to evaluate their impact on performance. The directory paths are
 * constructed to include detailed configuration settings such as the number of users, operation timeout,
 * work queue size, and other relevant metrics to facilitate easier identification and analysis of performance results.
 */
public class UserOperatorPerformanceReporter extends BasePerformanceReporter {
    private static final Logger LOGGER = LogManager.getLogger(UserOperatorPerformanceReporter.class);
    private static final String REPORT_DIRECTORY = "user-operator";

    /** Creates a builder for a User Operator capacity report. */
    public CapacityBuilder capacity() {
        return new CapacityBuilder();
    }
    /** Creates a builder for a User Operator scalability report. */
    public ScalabilityBuilder scalability() {
        return new ScalabilityBuilder();
    }
    /** Creates a builder for a User Operator latency report. */
    public LatencyBuilder latency() {
        return new LatencyBuilder();
    }

    /** Builder for User Operator capacity report attributes. */
    public final class CapacityBuilder {
        private String operationTimeoutMs;
        private String workQueueSize;
        private String controllerThreadPoolSize;
        private String cacheRefreshIntervalMs;
        private String batchQueueSize;
        private String batchMaximumBlockSize;
        private String batchMaximumBlockTimeMs;
        private int successfulCreations;
        private Map<Long, Map<String, List<Double>>> metricsHistory;
        public CapacityBuilder operationTimeoutMs(String value) {
            operationTimeoutMs = value;
            return this;
        }
        public CapacityBuilder workQueueSize(String value) {
            workQueueSize = value;
            return this;
        }
        public CapacityBuilder controllerThreadPoolSize(String value) {
            controllerThreadPoolSize = value;
            return this;
        }
        public CapacityBuilder cacheRefreshIntervalMs(String value) {
            cacheRefreshIntervalMs = value;
            return this;
        }
        public CapacityBuilder batchQueueSize(String value) {
            batchQueueSize = value;
            return this;
        }
        public CapacityBuilder batchMaximumBlockSize(String value) {
            batchMaximumBlockSize = value;
            return this;
        }
        public CapacityBuilder batchMaximumBlockTimeMs(String value) {
            batchMaximumBlockTimeMs = value;
            return this;
        }
        public CapacityBuilder successfulCreations(int value) {
            successfulCreations = value;
            return this;
        }
        public CapacityBuilder metricsHistory(Map<Long, Map<String, List<Double>>> value) {
            metricsHistory = value;
            return this;
        }
        public void log(TestStorage storage) throws IOException {
            UserOperatorPerformanceReporter.this.logPerformanceData(
                storage,
                attributes(),
                REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_CAPACITY_USE_CASE,
                performanceTimestamp(),
                performanceBaseDirectory()
            );
        }
        Map<String, Object> attributes() {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS, operationTimeoutMs);
            a.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, workQueueSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_CONTROLLER_THREAD_POOL_SIZE, controllerThreadPoolSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_CACHE_REFRESH_INTERVAL_MS, cacheRefreshIntervalMs);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_QUEUE_SIZE, batchQueueSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, batchMaximumBlockSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, batchMaximumBlockTimeMs);
            // Preserve the legacy report mapping: this key uses the controller thread pool size.
            a.put(PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE, controllerThreadPoolSize);
            a.put(PerformanceConstants.USER_OPERATOR_OUT_SUCCESSFUL_KAFKA_USERS_CREATED, successfulCreations);
            a.put(PerformanceConstants.METRICS_HISTORY, metricsHistory);
            return a;
        }
    }

    /** Builder for User Operator scalability report attributes. */
    public final class ScalabilityBuilder {
        private int workQueueSize, batchMaximumBlockSize, numberOfKafkaUsers;
        private int batchMaximumBlockTimeMs;
        private long reconciliationIntervalMs;
        public ScalabilityBuilder workQueueSize(int value) {
            workQueueSize = value;
            return this;
        }
        public ScalabilityBuilder batchMaximumBlockSize(int value) {
            batchMaximumBlockSize = value;
            return this;
        }
        public ScalabilityBuilder numberOfKafkaUsers(int value) {
            numberOfKafkaUsers = value;
            return this;
        }
        public ScalabilityBuilder batchMaximumBlockTimeMs(int value) {
            batchMaximumBlockTimeMs = value;
            return this;
        }
        public ScalabilityBuilder reconciliationIntervalMs(long value) {
            reconciliationIntervalMs = value;
            return this;
        }
        public void log(TestStorage storage) throws IOException {
            UserOperatorPerformanceReporter.this.logPerformanceData(
                storage,
                attributes(),
                REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE,
                performanceTimestamp(),
                performanceBaseDirectory()
            );
        }
        Map<String, Object> attributes() {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, workQueueSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, batchMaximumBlockSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, numberOfKafkaUsers);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, batchMaximumBlockTimeMs);
            a.put(PerformanceConstants.OPERATOR_OUT_RECONCILIATION_INTERVAL, reconciliationIntervalMs);
            return a;
        }
    }

    /** Builder for User Operator latency report attributes. */
    public final class LatencyBuilder {
        private int workQueueSize, batchMaximumBlockSize, numberOfKafkaUsers, batchMaximumBlockTimeMs;
        private LatencyMetrics latencyMetrics;
        private Map<Long, Map<String, List<Double>>> metricsHistory;
        public LatencyBuilder workQueueSize(int value) {
            workQueueSize = value;
            return this;
        }
        public LatencyBuilder batchMaximumBlockSize(int value) {
            batchMaximumBlockSize = value;
            return this;
        }
        public LatencyBuilder numberOfKafkaUsers(int value) {
            numberOfKafkaUsers = value;
            return this;
        }
        public LatencyBuilder batchMaximumBlockTimeMs(int value) {
            batchMaximumBlockTimeMs = value;
            return this;
        }
        public LatencyBuilder latencyMetrics(LatencyMetrics value) {
            latencyMetrics = value;
            return this;
        }
        public LatencyBuilder metricsHistory(Map<Long, Map<String, List<Double>>> value) {
            metricsHistory = value;
            return this;
        }
        public void log(TestStorage storage) throws IOException {
            UserOperatorPerformanceReporter.this.logPerformanceData(
                storage,
                attributes(),
                REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_LATENCY_USE_CASE,
                performanceTimestamp(),
                performanceBaseDirectory()
            );
        }
        Map<String, Object> attributes() {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, workQueueSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, batchMaximumBlockSize);
            a.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, numberOfKafkaUsers);
            a.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, batchMaximumBlockTimeMs);
            a.put(PerformanceConstants.OPERATOR_OUT_MIN_LATENCY, latencyMetrics.min());
            a.put(PerformanceConstants.OPERATOR_OUT_MAX_LATENCY, latencyMetrics.max());
            a.put(PerformanceConstants.OPERATOR_OUT_AVERAGE_LATENCY, latencyMetrics.average());
            a.put(PerformanceConstants.OPERATOR_OUT_P50_LATENCY, latencyMetrics.p50());
            a.put(PerformanceConstants.OPERATOR_OUT_P95_LATENCY, latencyMetrics.p95());
            a.put(PerformanceConstants.OPERATOR_OUT_P99_LATENCY, latencyMetrics.p99());
            a.put(PerformanceConstants.METRICS_HISTORY, metricsHistory);
            return a;
        }
    }

    /**
     * Resolves the directory path for storing performance logs based on the Kafka User Operator use case.
     * This overridden method constructs a unique directory path by incorporating various performance attributes,
     * making the logs easily traceable and aligned with specific test conditions.
     *
     * @param performanceLogDir         The base directory where performance logs are intended to be stored.
     * @param useCaseName               The name of the use case being tested, which helps categorize and organize the logs.
     * @param performanceAttributes     A map containing key performance metrics and configuration settings, which
     *                                  include the number of Kafka users, operation timeout, work queue size,
     *                                  thread pool size, cache refresh interval, batch queue size, batch block size,
     *                                  batch block time, and user operations thread pool size.
     * @return                          Path The fully resolved directory path where the specific use case's performance
     *                                  logs will be stored. This path is constructed to be unique and descriptive,
     *                                  incorporating key performance attributes.
     */
    @Override
    protected Path resolveComponentUseCasePathDir(Path performanceLogDir, String useCaseName, Map<String, Object> performanceAttributes) {
        // Extract parameters to make the directory name more descriptive and unique
        final String numberOfKafkaUsersToCreate = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, "").toString();
        final String threadPoolSize = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_CONTROLLER_THREAD_POOL_SIZE, "").toString();
        final String cacheRefreshInterval = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_CACHE_REFRESH_INTERVAL_MS, "").toString();
        final String batchQueueSize = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_BATCH_QUEUE_SIZE, "").toString();
        final String batchBlockSize = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, "").toString();
        final String batchBlockTime = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, "").toString();
        final String userOperationsThreadPoolSize = performanceAttributes.getOrDefault(PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE, "").toString();

        // Construct a directory name using the extracted parameters
        String directoryName = String.format("%s/users-%s-tp-%s-cache-%s-bq-%s-bb-%s-bt-%s-utp-%s",
            useCaseName,
            numberOfKafkaUsersToCreate,
            threadPoolSize,
            cacheRefreshInterval,
            batchQueueSize,
            batchBlockSize,
            batchBlockTime,
            userOperationsThreadPoolSize);

        final Path userOperatorUseCasePathDir = performanceLogDir.resolve(directoryName);

        // Log the resolved path for debugging purposes
        LOGGER.info("Resolved performance log directory: {} for use case '{}'.\nKafkaUsers: {},\nThreadPool: {},\nCacheInterval: {},\nBatchQueue: {},\nBatchBlock: {},\nBatchTime: {},\nUserOpsThreadPool: {}",
            userOperatorUseCasePathDir, useCaseName, numberOfKafkaUsersToCreate, threadPoolSize, cacheRefreshInterval, batchQueueSize, batchBlockSize, batchBlockTime, userOperationsThreadPoolSize);

        return userOperatorUseCasePathDir;
    }
}
