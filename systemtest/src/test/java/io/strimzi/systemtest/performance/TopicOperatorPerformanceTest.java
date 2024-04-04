/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicSpecBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.performance.gather.TopicOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.TopicOperatorPollingThread;
import io.strimzi.systemtest.performance.report.PerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.PerformanceMetricsParser;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
import io.strimzi.test.k8s.KubeClusterResource;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.strimzi.systemtest.TestConstants.PERFORMANCE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(PERFORMANCE)
public class TopicOperatorPerformanceTest extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorPerformanceTest.class);
    private static final int NUMBER_OF_MESSAGES = 10000;
    private static final TemporalAccessor ACTUAL_TIME = LocalDateTime.now();

    private static final String REPORT_DIRECTORY = "topic-operator";

    private TestStorage testStorage;
    private TopicOperatorMetricsCollector topicOperatorCollector;
    private TopicOperatorPollingThread topicOperatorPollingThread;
    private Thread daemonThread;

    /**
     * Provides configurations for Alice's bulk batch use case, testing different batch sizes and linger times.
     *
     * @return Stream of Arguments where:
     * - $1 - maxBatchSize (String): The maximum batch size for Kafka's topic operator batching process.
     * - $2 - maxBatchLingerMs (String): The maximum time, in milliseconds, the topic operator waits before processing batches.
     *
     * Configurations include:
     * - Short Linger Times ("10" ms): Useful for evaluating the impact of rapidly processing batches. This setting is beneficial for scenarios where immediate batch processing is required.
     * - Targeted Linger Time ("30000" ms or 30 seconds): Specifically introduced based on the scenario where a linger time of about 30 seconds is expected to balance batching efficiency without significantly affecting the overall operational duration. This setting is designed to simulate a real-world use case of creating 500 topics daily, optimizing for batching efficiency.
     * - Extended Linger Time ("100" ms): Provides a comparison to assess if longer wait times before processing batches could enhance performance, despite potentially increasing latency.
     *
     * This method supplies a variety of configurations to find the optimal balance between throughput and operational latency for bulk topic management in Kafka.
     */
    private static Stream<Arguments> provideConfigurationsForAliceBulkBatchUseCase() {
        // note: for single-node >= 30GB, >= 8CPUs -> each test takes ~45 minutes
        return Stream.of(
                // without clients
                Arguments.of("100", "10", false),     // Lower batch size with short linger time for comparison
                Arguments.of("500", "10", false),     // Increased batch size with short linger time
                Arguments.of("1000", "10", false),    // Large batch size with short linger time
                Arguments.of("100", "30000", false),  // Lower batch size with 30 seconds linger time
                Arguments.of("500", "30000", false),  // Increased batch size with 30 seconds linger time
                Arguments.of("1000", "30000", false), // Large batch size with 30 seconds linger time
                Arguments.of("100", "100", false),    // Lower batch size with longer linger time for extended comparison
                Arguments.of("500", "100", false),    // Increased batch size with longer linger time
                Arguments.of("1000", "100", false),    // Large batch size with longer linger time
                // with clients
                Arguments.of("100", "10", true),     // Lower batch size with short linger time for comparison
                Arguments.of("500", "10", true),     // Increased batch size with short linger time
                Arguments.of("1000", "10", true),    // Large batch size with short linger time
                Arguments.of("100", "30000", true),  // Lower batch size with 30 seconds linger time
                Arguments.of("500", "30000", true),  // Increased batch size with 30 seconds linger time
                Arguments.of("1000", "30000", true), // Large batch size with 30 seconds linger time
                Arguments.of("100", "100", true),    // Lower batch size with longer linger time for extended comparison
                Arguments.of("500", "100", true),    // Increased batch size with longer linger time
                Arguments.of("1000", "100", true)    // Large batch size with longer linger time
        );
    }

    /*
     * This test case is designed to simulate a bulk ingestion/batch use case, as might be required by a user like Alice.
     * Alice's scenario involves the need to rapidly create hundreds of topics, perform data ingestion in parallel across all these topics,
     * subsequently consume from them in parallel, and finally delete all the topics. This use case is particularly focused on minimizing
     * the latency associated with the bulk creation and deletion of topics. In such scenarios, Kafka's performance and its ability to handle
     * batching operations efficiently become crucial factors.
     *
     * The test leverages Kafka's batching capabilities for both creation and deletion of topics to aid in this process. By using a larger batch size
     * and a longer linger time, the test aims to optimize the throughput and minimize the operational latency. For instance, if the requirement is to
     * create 500 topics daily and Kafka takes approximately 5 minutes for this operation without considering the User Topic Operator (UTO),
     * setting a linger time of around 30 seconds would not significantly impact the overall duration but would help in batching efficiency.
     *
     * This test scenario is implemented to help users like Alice in understanding the performance characteristics of Kafka when dealing with
     * high volumes of topics and to assist in configuring their systems for optimal performance during bulk operations.
     */
    @ParameterizedTest
    @MethodSource("provideConfigurationsForAliceBulkBatchUseCase")
    @SuppressWarnings({"checkstyle:MethodLength"})
    public void testAliceBulkBatchUseCase(String maxBatchSize, String maxBatchLingerMs, boolean withClientsEnabled) throws IOException {
        final int numberOfTopics = KubeClusterResource.getInstance().isMultiNode() ? 500 : 250; // Number of topics to test
        final int numberOfClientInstances = withClientsEnabled ? 30 : 0; // producers and consumers if enabled
        final String topicNamePrefix = "perf-topic-";
        final String clientExchangeMessagesTopicPrefix = "client-topic-";
        final int brokerReplicas = KubeClusterResource.getInstance().isMultiNode() ? 5 : 3;
        final int controllerReplicas = 3;
        long startTimeMs, endTimeMs, createTopicsTimeMs = 0, endTimeWholeMs, totalTimeWholeMs = 0, totalDeletionTimeMs = 0, startSendRecvTimeMs = 0, endSendRecvTimeMs, totalSendAndRecvTimeMs = 0;

        try {
            resourceManager.createResourceWithWait(
                    NodePoolsConverter.convertNodePoolsIfNeeded(
                            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerReplicas).build(),
                            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerReplicas).build()
                    )
            );
            resourceManager.createResourceWithWait(
                KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
                KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerReplicas, controllerReplicas)
                    .editSpec()
                        .editKafka()
                            .withNewPersistentClaimStorage()
                                .withSize("10Gi")
                                .withDeleteClaim(true)
                            .endPersistentClaimStorage()
                        .endKafka()
                        .editEntityOperator()
                            .editTopicOperator()
                                .withReconciliationIntervalSeconds(10)
                            .endTopicOperator()
                            .editOrNewTemplate()
                                .editOrNewTopicOperatorContainer()
                                // Finalizers ensure orderly and controlled deletion of KafkaTopic resources.
                                // In this case we would delete them automatically via ResourceManager
                                    .addNewEnv()
                                        .withName("STRIMZI_USE_FINALIZERS")
                                        .withValue("false")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_ENABLE_ADDITIONAL_METRICS")
                                        .withValue("true")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_MAX_QUEUE_SIZE")
                                        .withValue(String.valueOf(Integer.MAX_VALUE))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_MAX_BATCH_SIZE")
                                        .withValue(maxBatchSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MAX_BATCH_LINGER_MS")
                                        .withValue(maxBatchLingerMs)
                                    .endEnv()
                                .endTopicOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                    .endSpec()
                    .build(),
                    ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

            this.testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
                    kubeClient().listPodsByPrefixInName(this.testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

            // Create topics related to clients a
            if (numberOfClientInstances != 0) {
                KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(), clientExchangeMessagesTopicPrefix,
                    numberOfClientInstances, 12, 3, 1);

                KafkaTopicScalabilityUtils.waitForTopicsReady(testStorage.getNamespaceName(), clientExchangeMessagesTopicPrefix, numberOfClientInstances);
            } // create KafkaUser for TLS communication via clients
            resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

            if (withClientsEnabled) {
                startSendRecvTimeMs = System.currentTimeMillis();

                spawnMultipleProducerAndConsumers(topicNamePrefix, numberOfClientInstances);
            }

            // -- Metrics POLL --
            // Assuming 'testStorage' contains necessary details like namespace and scraperPodName
            this.topicOperatorCollector = new TopicOperatorMetricsCollector.Builder()
                    .withScraperPodName(this.testStorage.getScraperPodName())
                    .withNamespaceName(this.testStorage.getNamespaceName())
                    .withComponentType(ComponentType.TopicOperator)
                    .withComponentName(this.testStorage.getClusterName())
                    .build();

            this.topicOperatorPollingThread = new TopicOperatorPollingThread(this.topicOperatorCollector, "strimzi.io/cluster=" + this.testStorage.getClusterName());
            this.daemonThread = new Thread(this.topicOperatorPollingThread);
            this.daemonThread.setDaemon(true); // Set as daemon so it doesn't prevent JVM shutdown
            this.daemonThread.start();
            // ----- ----- ------ ------

            // Measure topic creation time
            startTimeMs = System.currentTimeMillis();

            // Create topics
            KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(), topicNamePrefix,
                    numberOfTopics, 12, 3, 1);

            KafkaTopicScalabilityUtils.waitForTopicsReady(testStorage.getNamespaceName(), topicNamePrefix, numberOfTopics);

            endTimeMs = System.currentTimeMillis();
            createTopicsTimeMs = endTimeMs - startTimeMs;
            LOGGER.info("Time taken to create {} topics: {} ms", numberOfTopics, createTopicsTimeMs);

            endTimeWholeMs = System.currentTimeMillis();
            totalTimeWholeMs = endTimeWholeMs - startTimeMs;

            LOGGER.info("Time taken to create {} topics and send and recv: {} messages: {} ms", numberOfTopics, NUMBER_OF_MESSAGES, totalTimeWholeMs);

            if (withClientsEnabled) {
                // delete clients here
                for (int i = 0; i < numberOfClientInstances; i++) {
                    ClientUtils.waitForClientSuccess(testStorage.getProducerName() + "-" + i, testStorage.getNamespaceName(), NUMBER_OF_MESSAGES);
                    ClientUtils.waitForClientSuccess(testStorage.getConsumerName() + "-" + i, testStorage.getNamespaceName(), NUMBER_OF_MESSAGES);
                }
                endSendRecvTimeMs = System.currentTimeMillis();
                totalSendAndRecvTimeMs = endSendRecvTimeMs - startSendRecvTimeMs;

                LOGGER.info("Time taken send and recv: {} messages: {} ms", NUMBER_OF_MESSAGES, totalSendAndRecvTimeMs);
            }

            // Start measuring time for deletion of all topics
            long deletionStartTimeMs = System.currentTimeMillis();

            // Delete all KafkaTopics in the scope of the current test's extension context
            ResourceManager.getInstance().deleteResourcesOfType(KafkaTopic.RESOURCE_KIND);

            long deletionEndTime = System.currentTimeMillis();
            totalDeletionTimeMs = deletionEndTime - deletionStartTimeMs;
            LOGGER.info("Time taken to delete {} topics: {} ms", numberOfTopics, totalDeletionTimeMs);

            endTimeWholeMs = System.currentTimeMillis();
            totalTimeWholeMs = endTimeWholeMs - startTimeMs;

            LOGGER.info("Total time taken to create {} topics, send and receive {} messages, and delete topics: {} ms", numberOfTopics, NUMBER_OF_MESSAGES, totalTimeWholeMs);
        } finally {
            // Gracefully stop the topic-operator thread
            if (this.daemonThread != null) {
                // Step 1: Interrupt the daemon thread to stop collecting metrics
                this.daemonThread.interrupt();

                try {
                    // Step 2: Wait for the thread to finish, with a timeout
                    this.daemonThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Re-interrupt the current thread if interrupted while waiting
                    LOGGER.warn("Interrupted while waiting for the metrics collection thread to finish.", e);
                }

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
                performanceAttributes.put(PerformanceConstants.NUMBER_OF_TOPICS, numberOfTopics);
                performanceAttributes.put(PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES, numberOfClientInstances);
                performanceAttributes.put(PerformanceConstants.NUMBER_OF_MESSAGES, NUMBER_OF_MESSAGES);
                performanceAttributes.put(PerformanceConstants.CREATION_TIME, createTopicsTimeMs + " ms");
                performanceAttributes.put(PerformanceConstants.SEND_AND_RECV_TIME, totalSendAndRecvTimeMs + " ms");
                performanceAttributes.put(PerformanceConstants.DELETION_TIME, totalDeletionTimeMs + " ms");
                performanceAttributes.put(PerformanceConstants.TOTAL_TEST_TIME, totalTimeWholeMs + " ms");
                performanceAttributes.put(PerformanceConstants.MAX_BATCH_SIZE, maxBatchSize);
                performanceAttributes.put(PerformanceConstants.MAX_BATCH_LINGER_MS, maxBatchLingerMs);

                // Handling complex objects
                performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, topicOperatorPollingThread.getMetricsHistory()); // Map of metrics history

                // Step 3: Now, it's safe to log performance data as the collection thread has been stopped
                PerformanceReporter.logPerformanceData(performanceAttributes, TopicOperatorPerformanceTest.REPORT_DIRECTORY + "/" + PerformanceConstants.TOPIC_OPERATOR_ALICE_BULK_USE_CASE, this.testStorage, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
            }
        }
    }

     /**
      * Provides configurations for Bob's data streaming use case, focusing on supporting a large number of topics with minimal changes.
      *
      * Configurations aim to:
      * - Support scalability: Configurations are optimized to manage thousands of topics, considering memory usage and CPU utilization of the UTO.
      * - Minimize operational latency: By setting a reasonable linger time and batch size, the goal is to reduce the impact of small, infrequent changes without causing significant delays.
      *
      * The UTO's single-threaded nature means that beyond a certain point, increasing the number of topics primarily impacts memory usage and the time taken for no-op timed reconciliations. The configurations below are designed to find a balance that allows for scalability while maintaining responsiveness to changes.
      */
    private static Stream<Arguments> provideConfigurationsForBobDataStreamingUseCase() {
        return Stream.of(
            Arguments.of("250", "1000", false), // Medium batch size with 1 second linger time, optimized for infrequent changes
            Arguments.of("500", "1000", false), // Slightly larger batch size with 1 second linger time, considering memory usage efficiency
            Arguments.of("250", "5000", false), // Medium batch size with longer no-op timed reconciliation intervals for reduced CPU utilization
            Arguments.of("500", "5000", false)  // Larger batch size with longer intervals, balancing scalability with operational latency
        );
    }

    /**
     * Test for Bob's data streaming use case.
     *
     * This scenario simulates a production environment with thousands of topics where topics are updated infrequently.
     * The main objective is to ensure the UTO can handle the scale efficiently, with a goal of minimizing latency for
     * occasional, small changes.
     */
    @ParameterizedTest
    @MethodSource("provideConfigurationsForBobDataStreamingUseCase")
    @SuppressWarnings({"checkstyle:MethodLength"})
    public void testBobDataStreamingUseCase(String maxBatchSize, String maxBatchLingerMs, boolean withClientsEnabled) throws IOException, InterruptedException {
        final int numberOfTopics = KubeClusterResource.getInstance().isMultiNode() ? 3000 : 750; // Number of topics to test
        final int numberOfClientInstances = withClientsEnabled ? 30 : 0; // producers and consumers if enabled
        final String topicNamePrefix = "perf-topic-";
        final String clientExchangeMessagesTopicPrefix = "client-topic-";
        final int brokerReplicas = KubeClusterResource.getInstance().isMultiNode() ? 9 : 5;
        final int controllerReplicas = 3;
        long startTimeMs, endTimeMs, createTopicsTimeMs = 0, endTimeWholeMs, totalTimeWholeMs = 0, totalDeletionTimeMs = 0, startSendRecvTimeMs = 0, endSendRecvTimeMs, totalSendAndRecvTimeMs = 0;

        final int totalRounds = 3;
        final long[] bobUpdateTimerMsArr = new long[totalRounds];
        final int bobAmountOfKafkaTopics = 10; // Number of topics to update in each round

        try {
            resourceManager.createResourceWithWait(
                NodePoolsConverter.convertNodePoolsIfNeeded(
                    KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerReplicas).build(),
                    KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerReplicas).build()
                )
            );
            resourceManager.createResourceWithWait(
                KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
                KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerReplicas, controllerReplicas)
                    .editSpec()
                        .editKafka()
                            .withNewPersistentClaimStorage()
                                // 20Gi is not enough for multi-node (java.io.IOException: No space left on device)
                                .withSize(KubeClusterResource.getInstance().isMultiNode() ? "100Gi" : "20Gi")
                                .withDeleteClaim(true)
                            .endPersistentClaimStorage()
                        .endKafka()
                        .editEntityOperator()
                            .editTopicOperator()
                                .withReconciliationIntervalSeconds(10)
                            .endTopicOperator()
                            .editOrNewTemplate()
                                .editOrNewTopicOperatorContainer()
                                    // Finalizers ensure orderly and controlled deletion of KafkaTopic resources.
                                    // In this case we would delete them automatically via ResourceManager
                                    .addNewEnv()
                                        .withName("STRIMZI_USE_FINALIZERS")
                                        .withValue("false")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_ENABLE_ADDITIONAL_METRICS")
                                        .withValue("true")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_MAX_QUEUE_SIZE")
                                        .withValue(String.valueOf(Integer.MAX_VALUE))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_MAX_BATCH_SIZE")
                                        .withValue(maxBatchSize)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MAX_BATCH_LINGER_MS")
                                        .withValue(maxBatchLingerMs)
                                    .endEnv()
                                .endTopicOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                    .endSpec()
                    .build(),
                ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
            );

            this.testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
                kubeClient().listPodsByPrefixInName(this.testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

            // Create topics related to clients a
            if (numberOfClientInstances != 0) {
                KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(), clientExchangeMessagesTopicPrefix,
                    numberOfClientInstances, 12, 3, 1);

                KafkaTopicScalabilityUtils.waitForTopicsReady(testStorage.getNamespaceName(), clientExchangeMessagesTopicPrefix, numberOfClientInstances);
            }
            // create KafkaUser for TLS communication via clients
            resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

            if (withClientsEnabled) {
                startSendRecvTimeMs = System.currentTimeMillis();

                spawnMultipleProducerAndConsumers(topicNamePrefix, numberOfClientInstances);
            }

            // -- Metrics POLL --
            // Assuming 'testStorage' contains necessary details like namespace and scraperPodName
            this.topicOperatorCollector = new TopicOperatorMetricsCollector.Builder()
                .withScraperPodName(this.testStorage.getScraperPodName())
                .withNamespaceName(this.testStorage.getNamespaceName())
                .withComponentType(ComponentType.TopicOperator)
                .withComponentName(this.testStorage.getClusterName())
                .build();

            this.topicOperatorPollingThread = new TopicOperatorPollingThread(this.topicOperatorCollector, "strimzi.io/cluster=" + this.testStorage.getClusterName());
            this.daemonThread = new Thread(this.topicOperatorPollingThread);
            this.daemonThread.setDaemon(true); // Set as daemon so it doesn't prevent JVM shutdown
            this.daemonThread.start();
            // ----- ----- ------ ------

            // Measure topic creation time
            startTimeMs = System.currentTimeMillis();

            // Create topics
            KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(), topicNamePrefix,
                numberOfTopics, 12, 5, 3);

            KafkaTopicScalabilityUtils.waitForTopicsReady(testStorage.getNamespaceName(), topicNamePrefix, numberOfTopics);

            endTimeMs = System.currentTimeMillis();
            createTopicsTimeMs = endTimeMs - startTimeMs;
            LOGGER.info("Time taken to create {} topics: {} ms", numberOfTopics, createTopicsTimeMs);

            endTimeWholeMs = System.currentTimeMillis();
            totalTimeWholeMs = endTimeWholeMs - startTimeMs;

            LOGGER.info("Time taken to create {} topics and send and recv: {} messages: {} ms", numberOfTopics, NUMBER_OF_MESSAGES, totalTimeWholeMs);

            if (withClientsEnabled) {
                // delete clients here
                for (int i = 0; i < numberOfClientInstances; i++) {
                    ClientUtils.waitForClientSuccess(testStorage.getProducerName() + "-" + i, testStorage.getNamespaceName(), NUMBER_OF_MESSAGES);
                    ClientUtils.waitForClientSuccess(testStorage.getConsumerName() + "-" + i, testStorage.getNamespaceName(), NUMBER_OF_MESSAGES);
                }
                endSendRecvTimeMs = System.currentTimeMillis();
                totalSendAndRecvTimeMs = endSendRecvTimeMs - startSendRecvTimeMs;

                LOGGER.info("Time taken send and recv: {} messages: {} ms", NUMBER_OF_MESSAGES, totalSendAndRecvTimeMs);
            }

            // ----------  MAIN PART -----------
            // --------> small changes <--------
            List<Map<String, Object>> roundConfigs = List.of(
                // Round 1 Config
                Map.of(
                    "compression.type", "gzip",
                    "cleanup.policy", "delete",
                    "message.timestamp.type", "LogAppendTime",
                    "min.insync.replicas", 2
                ),
                // Round 2 Config
                Map.of(
                    "max.compaction.lag.ms", 54321L,
                    "min.compaction.lag.ms", 54L,
                    "retention.ms", 3690L,
                    "segment.ms", 123456L,
                    "flush.ms", 456123L
                ),
                // Round 3 Config
                Map.of(
                    "retention.bytes", 9876543L,
                    "segment.bytes", 321654L,
                    "max.message.bytes", 654321L,
                    "flush.messages", 456123L
                )
            );

            // Assuming you're within a loop or method that iterates through each round
            for (int round = 0; round < totalRounds; round++) {
                Map<String, Object> currentConfig = roundConfigs.get(round);
                // Now, `currentConfig` holds the configuration for the current round.
                // You can use `currentConfig` for updating topics in this round.

                long timeTakenForRound = OperationTimer.measureTimeInMillis(() -> {
                    KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(
                        testStorage.getNamespaceName(),
                        topicNamePrefix, // Ensure this is defined or passed appropriately
                        bobAmountOfKafkaTopics, // Number of topics to update, adjust as needed
                        new KafkaTopicSpecBuilder().withConfig(currentConfig).build()
                    );
                    // Wait for topics to reflect the updated configurations, or for readiness
                    KafkaTopicScalabilityUtils.waitForTopicsContainConfig(
                        testStorage.getNamespaceName(),
                        topicNamePrefix, // Ensure this is defined or passed appropriately
                        bobAmountOfKafkaTopics, // Number of topics, adjust as needed
                        currentConfig
                    );
                });

                bobUpdateTimerMsArr[round] = timeTakenForRound; // Store the time taken for this round
                LOGGER.info("Round {}: Time taken to update {} topics: {} ms", round, bobAmountOfKafkaTopics, timeTakenForRound);

                // Optionally pause between rounds, if required
                TimeUnit.MINUTES.sleep(1);
            }

            // Start measuring time for deletion of all topics
            long deletionStartTimeMs = System.currentTimeMillis();

            // Delete all KafkaTopics in the scope of the current test's extension context
            ResourceManager.getInstance().deleteResourcesOfType(KafkaTopic.RESOURCE_KIND);

            long deletionEndTime = System.currentTimeMillis();
            totalDeletionTimeMs = deletionEndTime - deletionStartTimeMs;
            LOGGER.info("Time taken to delete {} topics: {} ms", numberOfTopics, totalDeletionTimeMs);

            endTimeWholeMs = System.currentTimeMillis();
            totalTimeWholeMs = endTimeWholeMs - startTimeMs;

            LOGGER.info("Total time taken to create {} topics, send and receive {} messages, and delete topics: {} ms", numberOfTopics, NUMBER_OF_MESSAGES, totalTimeWholeMs);
        } finally {
            // Gracefully stop the topic-operator thread
            if (this.daemonThread != null) {
                // Step 1: Interrupt the daemon thread to stop collecting metrics
                this.daemonThread.interrupt();

                try {
                    // Step 2: Wait for the thread to finish, with a timeout
                    this.daemonThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Re-interrupt the current thread if interrupted while waiting
                    LOGGER.warn("Interrupted while waiting for the metrics collection thread to finish.", e);
                }

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
                performanceAttributes.put(PerformanceConstants.NUMBER_OF_TOPICS, numberOfTopics);
                performanceAttributes.put(PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES, numberOfClientInstances);
                performanceAttributes.put(PerformanceConstants.NUMBER_OF_MESSAGES, NUMBER_OF_MESSAGES);
                performanceAttributes.put(PerformanceConstants.CREATION_TIME, createTopicsTimeMs + " ms");
                performanceAttributes.put(PerformanceConstants.SEND_AND_RECV_TIME, totalSendAndRecvTimeMs + " ms");
                performanceAttributes.put(PerformanceConstants.DELETION_TIME, totalDeletionTimeMs + " ms");
                performanceAttributes.put(PerformanceConstants.TOTAL_TEST_TIME, totalTimeWholeMs + " ms");
                performanceAttributes.put(PerformanceConstants.MAX_BATCH_SIZE, maxBatchSize);
                performanceAttributes.put(PerformanceConstants.MAX_BATCH_LINGER_MS, maxBatchLingerMs);
                performanceAttributes.put(PerformanceConstants.BOB_NUMBER_OF_TOPICS_TO_UPDATE, bobAmountOfKafkaTopics);

                // Handling complex objects
                performanceAttributes.put(PerformanceConstants.BOB_UPDATE_TIMES, bobUpdateTimerMsArr); // Array of update times
                performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, topicOperatorPollingThread.getMetricsHistory()); // Map of metrics history
                // Step 3: Now, it's safe to log performance data as the collection thread has been stopped
                PerformanceReporter.logPerformanceData(performanceAttributes, TopicOperatorPerformanceTest.REPORT_DIRECTORY + "/" + PerformanceConstants.TOPIC_OPERATOR_BOBS_STREAMING_USE_CASE, this.testStorage, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
            }
        }
    }

    private void spawnMultipleProducerAndConsumers(final String topicNamePrefix, final int numberOfClientInstances) {
        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withUsername(testStorage.getUsername())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(NUMBER_OF_MESSAGES)
            // 100 mgs/s if possible
            .withDelayMs(10)
            .build();

        // Launching 30 producers and consumers without waiting for them to complete
        for (int i = 0; i < numberOfClientInstances; i++) {
            clients = new KafkaClientsBuilder(clients)
                    .withProducerName(testStorage.getProducerName() + "-" + i)
                    .withConsumerName(testStorage.getConsumerName() + "-" + i)
                    .withTopicName(topicNamePrefix + i)
                    .build();

            resourceManager.createResourceWithoutWait(
                    clients.producerTlsStrimzi(testStorage.getClusterName()),
                    clients.consumerTlsStrimzi(testStorage.getClusterName()));
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
     void tearDown() throws IOException {
        // show tables with metrics
        PerformanceMetricsParser.main(null);
    }
}

