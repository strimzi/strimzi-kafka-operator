/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.logs.LogCollector;
import io.strimzi.systemtest.metrics.UserOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.UserOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.UserOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.UserOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.TopicOperatorMetricsParser;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.systemtest.TestConstants.PERFORMANCE;
import static io.strimzi.systemtest.performance.PerformanceConstants.PERFORMANCE_CAPACITY;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Tag(PERFORMANCE)
public class UserOperatorPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorPerformance.class);
    private static final TemporalAccessor ACTUAL_TIME = LocalDateTime.now();

    private static final String REPORT_DIRECTORY = "user-operator";

    private TestStorage testStorage;
    private UserOperatorMetricsCollector userOperatorCollector;
    private UserOperatorMetricsCollectionScheduler userOperatorMetricsGatherer;
    private UserOperatorPerformanceReporter userOperatorPerformanceReporter = new UserOperatorPerformanceReporter();
    private LogCollector logCollector;

    /**
     * Provides a stream of configurations for parameterized testing of Kafka User Operator's bulk batch operations.
     * This method tests different operational parameters to evaluate their impact on performance when creating
     * Kafka users in bulk. The configurations are designed to assess performance under various conditions,
     * including different queue sizes, batch settings, and timeout durations.
     *
     * Configurations are defined for two scenarios:
     * 1. Low bulk batch creation with 100 Kafka users
     * 2. Medium bulk batch creation with 500 Kafka users
     *
     * Each test configuration varies the following parameters:
     * - Number of Kafka users to create
     * - Controller thread pool size
     * - Cache refresh interval (ms)
     * - Batch queue size
     * - Maximum batch block size
     * - Maximum batch block time (ms)
     * - User operations thread pool size
     *
     * @return      a stream of {@link Arguments} instances, each representing a set of parameters for the test.
     */
    private static Stream<Arguments> provideConfigurationsForBulkBatchUseCase() {
        return Stream.of(
            // Configurations for low bulk batch creation (100 users)
            Arguments.of(100, "50", "15000", "1024", "100", "100", "4"),  // Default configuration
            Arguments.of(100, "100", "10000", "2048", "200", "50", "10"), // High throughput configuration
            Arguments.of(100, "50", "15000", "1024", "100", "200", "4"),  // High batch time configuration
            Arguments.of(100,  "25", "30000", "512", "50", "100", "2"),     // Lower performance, higher timeout
            Arguments.of(100, "100", "5000", "4096", "500", "10", "20"),   // Extremely high performance configuration

            // Configurations for medium bulk batch creation (500 users)
            Arguments.of(500, "50", "15000", "1024", "100", "100", "4"),  // Default configuration
            Arguments.of(500, "100", "10000", "2048", "200", "50", "10"), // High throughput configuration
            Arguments.of(500, "50", "15000", "1024", "100", "200", "4"),  // High batch time configuration
            Arguments.of(500,  "25", "30000", "512", "50", "100", "2"),     // Lower performance, higher timeout
            Arguments.of(500, "100", "5000", "4096", "500", "10", "20")   // Extremely high performance configuration
        );
    }

    //  Queue Sizes and Thread Pools:
    //      Larger WORK_QUEUE_SIZE, BATCH_QUEUE_SIZE, and CONTROLLER_THREAD_POOL_SIZE, particularly in combination with
    //      higher BATCH_MAXIMUM_BLOCK_SIZE and lower BATCH_MAXIMUM_BLOCK_TIME_MS, generally result in faster operations
    //      for 100 users. However, for 500 users, while the operation time increases, the system load and memory usage
    //      also increase, suggesting a trade-off between performance and resource usage.
    //  Operation Timeout and Cache Refresh Interval:
    //      These seem less correlated with direct performance impacts in terms of creation/deletion times but are crucial
    //      for handling errors and maintaining data consistency. Longer timeouts and shorter refresh intervals might be
    //      beneficial in very large-scale environments to avoid timeouts and stale data.
    @ParameterizedTest
    @MethodSource("provideConfigurationsForBulkBatchUseCase")
    public void testAliceBulkBatchUseCase(int numberOfKafkaUsersToCreate, String controllerThreadPoolSize,
                                      String cacheRefreshIntervalMs, String batchQueueSize, String batchMaximumBlockSize,
                                      String batchMaximumBlockTimeMs, String userOperationsThreadPoolSize) throws IOException {
        final int brokerReplicas = 3;
        final int controllerReplicas = 3;
        long creationUsersMs = 0;
        long deletionUsersMs = 0;
        // we set worker queue size to high number as we measure performance and not memory or sizing...
        final String workerQueueSize = "10000";

        try {
            resourceManager.createResourceWithWait(
                NodePoolsConverter.convertNodePoolsIfNeeded(
                    KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerReplicas)
                        .editSpec()
                            .withNewPersistentClaimStorage()
                                .withSize("10Gi")
                            .endPersistentClaimStorage()
                        .endSpec()
                        .build(),
                    KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerReplicas).build()
                )
            );
            resourceManager.createResourceWithWait(
                KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
                KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerReplicas, controllerReplicas)
                    .editSpec()
                        .editEntityOperator()
                            .editUserOperator()
                                .withReconciliationIntervalMs(10_000L)
                            .endUserOperator()
                            .editOrNewTemplate()
                                .editOrNewUserOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_OPERATION_TIMEOUT_MS")
                                        .withValue("300000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_WORK_QUEUE_SIZE")
                                        .withValue(workerQueueSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_CONTROLLER_THREAD_POOL_SIZE")
                                        .withValue(controllerThreadPoolSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_CACHE_REFRESH_INTERVAL_MS")
                                        .withValue(cacheRefreshIntervalMs)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_QUEUE_SIZE")
                                        .withValue(batchQueueSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE")
                                        .withValue(batchMaximumBlockSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS")
                                        .withValue(batchMaximumBlockTimeMs)
                                    .endEnv()
                                        .addNewEnv()
                                        .withName("STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE")
                                        .withValue(userOperationsThreadPoolSize)
                                    .endEnv()
                                .endUserOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                        .editKafka()
                            .withNewKafkaAuthorizationSimple()
                            .endKafkaAuthorizationSimple()
                        .endKafka()
                    .endSpec()
                    .build(),
                ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
            );

            this.testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
                kubeClient().listPodsByPrefixInName(this.testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

            // -- Metrics POLL --
            // Assuming 'testStorage' contains necessary details like namespace and scraperPodName
            this.userOperatorCollector = new UserOperatorMetricsCollector.Builder()
                .withScraperPodName(this.testStorage.getScraperPodName())
                .withNamespaceName(this.testStorage.getNamespaceName())
                .withComponent(UserOperatorMetricsComponent.create(this.testStorage.getNamespaceName(), this.testStorage.getClusterName()))
                .build();

            this.userOperatorMetricsGatherer = new UserOperatorMetricsCollectionScheduler(this.userOperatorCollector, "strimzi.io/cluster=" + this.testStorage.getClusterName());
            this.userOperatorMetricsGatherer.startCollecting();

            creationUsersMs = OperationTimer.measureTimeInMillis(() -> {
                List<KafkaUser> usersList = UserOperatorPerformanceUtils.getListOfKafkaUsers(this.testStorage, testStorage.getUsername(), numberOfKafkaUsersToCreate, UserAuthType.Tls); // TODO: check this with TLS, SCramsha nad external....

                UserOperatorPerformanceUtils.createAllUsersInListWithWait(testStorage, usersList, testStorage.getUsername());
            });

            LOGGER.info("Time taken to create {} topics: {} ms", numberOfKafkaUsersToCreate, creationUsersMs);


            // Start measuring time for deletion of all users
            LOGGER.info("Start deletion of {} KafkaUsers in namespace:{}", numberOfKafkaUsersToCreate, testStorage.getNamespaceName());

            deletionUsersMs = OperationTimer.measureTimeInMillis(() -> {
                resourceManager.deleteResourcesOfTypeWithoutWait(KafkaUser.RESOURCE_KIND);
                KafkaUserUtils.waitForUserWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getUsername());
            });

            LOGGER.info("Time taken to delete {} topics: {} ms", numberOfKafkaUsersToCreate, deletionUsersMs);
        } finally {
            if (this.userOperatorMetricsGatherer != null) {
                this.userOperatorMetricsGatherer.stopCollecting();

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_NUMBER_OF_KAFKA_USERS, numberOfKafkaUsersToCreate);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS, "300000");
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, workerQueueSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_CONTROLLER_THREAD_POOL_SIZE, controllerThreadPoolSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_CACHE_REFRESH_INTERVAL_MS, cacheRefreshIntervalMs);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_QUEUE_SIZE, batchQueueSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, batchMaximumBlockSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, batchMaximumBlockTimeMs);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE, userOperationsThreadPoolSize);

                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_OUT_CREATION_TIME, creationUsersMs);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_OUT_DELETION_TIME, deletionUsersMs);

                // Handling complex objects
                performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, this.userOperatorMetricsGatherer.getMetricsStore()); // Map of metrics history

                // Step 3: Now, it's safe to log performance data as the collection thread has been stopped
                this.userOperatorPerformanceReporter.logPerformanceData(this.testStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.USER_OPERATOR_ALICE_BULK_USE_CASE, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
            }
        }
    }

    /**
     * Provides a stream of configurations for capacity testing, designed to evaluate
     * different operational parameters and their impact on system performance. Each configuration
     * is tailored to test various aspects of the system under conditions that mimic different
     * operational requirements, such as varying levels of parallel processing, batching strategies,
     * and operational latencies.
     *
     * Parameters:
     * $1 - Controller thread pool size: Defines the size of the thread pool used by the controller.
     *       Larger sizes allow more concurrent operations but require more system resources.
     * $2 - Cache refresh interval (ms): Time interval in milliseconds between consecutive cache refreshes,
     *       affecting how current the data seen by the controller is.
     * $3 - Batch queue size: Specifies the size of the batch queue, influencing how many operations
     *       can be batched together before being processed.
     * $4 - Maximum batch block size: The maximum number of operations that can be included in a single batch,
     *       affecting throughput and latency.
     * $5 - Maximum batch block time (ms): The maximum time in milliseconds that a batch can be held before
     *       processing must start, balancing between immediate and delayed execution for better resource usage.
     * $6 - User operations thread pool size: Specifies the number of threads dedicated to user operations,
     *       impacting the concurrency level of user-specific tasks.
     *
     * @return a stream of {@link Arguments} instances, each representing a set of parameters for the test.
     */
    private static Stream<Arguments> provideConfigurationsForCapacity() {
        return Stream.of(
            // Default configuration (Successfully deployed 23,900 users.)
            Arguments.of("50", "15000", "1024", "100", "100", "4")
            //  Enhanced Parallel Processing (Successfully provisioned 44,100 users.)
//            Arguments.of("100", "20000", "2048", "200", "50", "10"),
            // Conservative Batching (Managed to support 46,200 users before the machine experienced a crash but with stronger machine 55100 users with UO 12 GiB memory consumption )
//            Arguments.of("75", "15000", "1500", "150", "75", "8"),
            // Aggressive Batching (very similar to Conservative Batching)
//            Arguments.of("100", "30000", "4096", "300", "100", "12")
        );
    }

    @Tag(PERFORMANCE_CAPACITY)
    @ParameterizedTest
    @MethodSource("provideConfigurationsForCapacity")
    void testCapacity(String controllerThreadPoolSize, String cacheRefreshIntervalMs, String batchQueueSize,
                      String batchMaximumBlockSize, String batchMaximumBlockTimeMs, String userOperationsThreadPoolSize) throws IOException {
        final int brokerReplicas = 3;
        final int controllerReplicas = 3;
        int successfulCreations = 0;
        // we set worker queue size to high number as we measure performance and not memory or sizing...
        final String workerQueueSize = "10000";

        try {
            resourceManager.createResourceWithWait(
                NodePoolsConverter.convertNodePoolsIfNeeded(
                    KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerReplicas)
                        .editSpec()
                            .withNewPersistentClaimStorage()
                                .withSize("10Gi")
                            .endPersistentClaimStorage()
                        .endSpec()
                        .build(),
                    KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerReplicas).build()
                )
            );
            resourceManager.createResourceWithWait(
                KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
                KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerReplicas, controllerReplicas)
                    .editSpec()
                        .editEntityOperator()
                            .editUserOperator()
                                .withReconciliationIntervalMs(10_000L)
                            .endUserOperator()
                            .editOrNewTemplate()
                                .editOrNewUserOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_WORK_QUEUE_SIZE")
                                        .withValue(workerQueueSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_CONTROLLER_THREAD_POOL_SIZE")
                                        .withValue(controllerThreadPoolSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_CACHE_REFRESH_INTERVAL_MS")
                                        .withValue(cacheRefreshIntervalMs)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_QUEUE_SIZE")
                                        .withValue(batchQueueSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE")
                                        .withValue(batchMaximumBlockSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS")
                                        .withValue(batchMaximumBlockTimeMs)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE")
                                        .withValue(userOperationsThreadPoolSize)
                                    .endEnv()
                                .endUserOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                        .editKafka()
                            .withNewKafkaAuthorizationSimple()
                            .endKafkaAuthorizationSimple()
                        .endKafka()
                    .endSpec()
                    .build(),
                ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
            );

            this.testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
                kubeClient().listPodsByPrefixInName(this.testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

            // -- Metrics POLL --
            // Assuming 'testStorage' contains necessary details like namespace and scraperPodName
            this.userOperatorCollector = new UserOperatorMetricsCollector.Builder()
                .withScraperPodName(this.testStorage.getScraperPodName())
                .withNamespaceName(this.testStorage.getNamespaceName())
                .withComponent(UserOperatorMetricsComponent.create(this.testStorage.getNamespaceName(), this.testStorage.getClusterName()))
                .build();

            this.userOperatorMetricsGatherer = new UserOperatorMetricsCollectionScheduler(this.userOperatorCollector, "strimzi.io/cluster=" + this.testStorage.getClusterName());
            this.userOperatorMetricsGatherer.startCollecting();

            // we will create incrementally users
            final int batchSize = 100;

            while (true) { // Endless loop
                int start = successfulCreations;
                int end = successfulCreations + batchSize;
                List<KafkaUser> users = UserOperatorPerformanceUtils.getListOfKafkaUsers(this.testStorage, this.testStorage.getUsername(), start, end, UserAuthType.Tls);
                try {
                    UserOperatorPerformanceUtils.createAllUsersInListWithWait(this.testStorage, users, this.testStorage.getUsername());
                    successfulCreations += batchSize;
                    LOGGER.info("Successfully created and verified batch from {} to {}", start, end);
                } catch (WaitException e) {
                    LOGGER.error("Failed to create Kafka users from index {} to {}: {}", start, end, e.getMessage());

                    // after a failure we will gather logs from all components under test (i.e., UO, Kafka pods) to observer behaviour
                    // what might be a bottleneck of such performance
                    this.logCollector = new LogCollector();
                    this.logCollector.collect();

                    break; // Break out of the loop if an error occurs
                }
            }
        } finally {
            // to enchantment a process of deleting we should delete all resources at once
            // I saw a behaviour where deleting one by one might lead to 10s delay for deleting each KafkaUser
            resourceManager.deleteResourcesOfTypeWithoutWait(KafkaUser.RESOURCE_KIND);
            KafkaUserUtils.waitForUserWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getUsername());

            if (this.userOperatorMetricsGatherer != null) {
                this.userOperatorMetricsGatherer.stopCollecting();

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();

                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_OPERATION_TIMEOUT_MS, "300000");
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_WORK_QUEUE_SIZE, workerQueueSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_CONTROLLER_THREAD_POOL_SIZE, controllerThreadPoolSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_CACHE_REFRESH_INTERVAL_MS, cacheRefreshIntervalMs);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_QUEUE_SIZE, batchQueueSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_SIZE, batchMaximumBlockSize);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_BATCH_MAXIMUM_BLOCK_TIME_MS, batchMaximumBlockTimeMs);
                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_IN_USER_OPERATIONS_THREAD_POOL_SIZE, controllerThreadPoolSize);

                performanceAttributes.put(PerformanceConstants.USER_OPERATOR_OUT_SUCCESSFUL_KAFKA_USERS_CREATED, successfulCreations);

                performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, this.userOperatorMetricsGatherer.getMetricsStore()); // Map of metrics history

                this.userOperatorPerformanceReporter.logPerformanceData(this.testStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_CAPACITY_USE_CASE, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
            }
        }
    }

    @BeforeEach
    public void setUp(ExtensionContext extensionContext) {
        this.testStorage = new TestStorage(extensionContext, TestConstants.CO_NAMESPACE);
    }

    // Additional setup and utility methods as needed
    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }

    @AfterAll
    void tearDown() {
        // show tables with metrics
        TopicOperatorMetricsParser.main(new String[]{PerformanceConstants.USER_OPERATOR_PARSER});
    }
}
