/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
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
        LOGGER.info("Resolved performance log directory: {} for use case '{}'. KafkaUsers: {}, ThreadPool: {}, CacheInterval: {}, BatchQueue: {}, BatchBlock: {}, BatchTime: {}, UserOpsThreadPool: {}",
            userOperatorUseCasePathDir, useCaseName, numberOfKafkaUsersToCreate, threadPoolSize, cacheRefreshInterval, batchQueueSize, batchBlockSize, batchBlockTime, userOperationsThreadPoolSize);

        return userOperatorUseCasePathDir;
    }
}
