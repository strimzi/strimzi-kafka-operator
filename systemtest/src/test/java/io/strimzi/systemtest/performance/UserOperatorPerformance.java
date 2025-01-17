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
import io.strimzi.systemtest.logs.TestLogCollector;
import io.strimzi.systemtest.metrics.UserOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.UserOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.UserOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.UserOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.TopicOperatorMetricsParser;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils;
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

import static io.strimzi.systemtest.TestTags.CAPACITY;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class UserOperatorPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorPerformance.class);
    private static final TemporalAccessor ACTUAL_TIME = LocalDateTime.now();

    private static final String REPORT_DIRECTORY = "user-operator";

    private TestStorage testStorage;
    private UserOperatorMetricsCollector userOperatorCollector;
    private UserOperatorMetricsCollectionScheduler userOperatorMetricsGatherer;
    private UserOperatorPerformanceReporter userOperatorPerformanceReporter = new UserOperatorPerformanceReporter();
    private TestLogCollector logCollector;

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

    @Tag(CAPACITY)
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
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerReplicas)
                    .editSpec()
                        .withNewPersistentClaimStorage()
                            .withSize("10Gi")
                        .endPersistentClaimStorage()
                    .endSpec()
                    .build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerReplicas).build()
            );
            resourceManager.createResourceWithWait(
                KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
                KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerReplicas)
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
                    this.logCollector = new TestLogCollector();
                    this.logCollector.collectLogs();

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
