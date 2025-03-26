/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.logs.TestLogCollector;
import io.strimzi.systemtest.metrics.TopicOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.TopicOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.TopicOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.TopicOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.TopicOperatorMetricsParser;
import io.strimzi.systemtest.performance.utils.TopicOperatorPerformanceUtils;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
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
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.systemtest.TestTags.TOPIC_CAPACITY;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class TopicOperatorPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorPerformance.class);

    private TestStorage testStorage;
    private TopicOperatorMetricsCollector topicOperatorCollector;
    private TopicOperatorMetricsCollectionScheduler topicOperatorMetricsGatherer;
    private TestLogCollector logCollector;

    protected static final TemporalAccessor ACTUAL_TIME = LocalDateTime.now();
    protected static final String REPORT_DIRECTORY = "topic-operator";

    protected TopicOperatorPerformanceReporter topicOperatorPerformanceReporter = new TopicOperatorPerformanceReporter();

    /**
     * Provides a default of configurations for capacity test
     * Configuration is designed to test the impact of different max batch sizes and linger times
     * on the system's throughput and responsiveness.
     *
     * @return Stream of Arguments where each pair consists of:
     *         - Max Batch Size (String): The maximum number of topic operations (e.g., creation, deletion)
     *           that the Topic Operator will batch together before processing.
     *         - Max Batch Linger ms (String): The maximum time, in milliseconds, that the Topic Operator
     *           will wait before processing a batch, allowing more operations to accumulate and thus increasing
     *           batching efficiency.
     *
     * Configurations include:
     * - Default configuration: A balanced setup for typical use cases.
     */
    private static Stream<Arguments> provideConfigurationsForCapacity() {
        return Stream.of(
            Arguments.of("100", "100")     // Default configuration
        );
    }

    @Tag(TOPIC_CAPACITY)
    @ParameterizedTest
    @MethodSource("provideConfigurationsForCapacity")
    void testCapacity(String maxBatchSize, String maxBatchLingerMs) throws IOException {
        final int brokerReplicas = 3;
        final int controllerReplicas = 3;
        int successfulCreations = 0;
        final String maxQueueSize = String.valueOf(Integer.MAX_VALUE);

        try {
            resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerReplicas)
                    .editSpec()
                        .withNewPersistentClaimStorage()
                            .withSize("50Gi")
                        .endPersistentClaimStorage()
                    .endSpec()
                    .build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerReplicas)
                    .editSpec()
                        .withNewPersistentClaimStorage()
                            .withSize("5Gi")
                        .endPersistentClaimStorage()
                    .endSpec()
                    .build()
            );
            resourceManager.createResourceWithWait(
                KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
                KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerReplicas)
                    .editSpec()
                        .editEntityOperator()
                            .editTopicOperator()
                                .withReconciliationIntervalMs(10_000L)
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
                                        .withValue(maxQueueSize)
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

            // -- Metrics POLL --
            // Assuming 'testStorage' contains necessary details like namespace and scraperPodName
            this.topicOperatorCollector = new TopicOperatorMetricsCollector.Builder()
                .withScraperPodName(this.testStorage.getScraperPodName())
                .withNamespaceName(this.testStorage.getNamespaceName())
                .withComponent(TopicOperatorMetricsComponent.create(this.testStorage.getNamespaceName(), this.testStorage.getClusterName()))
                .build();

            this.topicOperatorMetricsGatherer = new TopicOperatorMetricsCollectionScheduler(this.topicOperatorCollector, "strimzi.io/cluster=" + this.testStorage.getClusterName());
            this.topicOperatorMetricsGatherer.startCollecting();

            // we will create incrementally topics
            final int batchSize = 100;
            final int topicPartitions = 12;
            final int topicReplicas = 3;
            final int minInSyncReplicas = 2;

            while (true) { // Endless loop
                int start = successfulCreations;
                int end = successfulCreations + batchSize;

                try {
                    // Create topics
                    KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(),
                        testStorage.getTopicName(), start, end, topicPartitions, topicReplicas, minInSyncReplicas);
                    KafkaTopicScalabilityUtils.waitForTopicStatus(testStorage.getNamespaceName(), testStorage.getTopicName(),
                        start, end, CustomResourceStatus.Ready, ConditionStatus.True);

                    successfulCreations += batchSize;
                    LOGGER.info("Successfully created and verified batch from {} to {}", start, end);
                } catch (WaitException e) {
                    LOGGER.error("Failed to create Kafka topics from index {} to {}: {}", start, end, e.getMessage());

                    // after a failure we will gather logs from all components under test (i.e., TO, Kafka pods) to observer behaviour
                    // what might be a bottleneck of such performance
                    this.logCollector = new TestLogCollector();
                    this.logCollector.collectLogs();

                    break; // Break out of the loop if an error occurs
                }
            }
        } finally {
            // to enchantment a process of deleting we should delete all resources at once
            // I saw a behaviour where deleting one by one might lead to 10s delay for deleting each KafkaTopic
            LOGGER.info("Start deletion KafkaTopics in namespace:{}", testStorage.getNamespaceName());
            resourceManager.deleteResourcesOfTypeWithoutWait(KafkaTopic.RESOURCE_KIND);
            KafkaTopicUtils.waitForTopicWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getTopicName());

            if (this.topicOperatorMetricsGatherer != null) {
                this.topicOperatorMetricsGatherer.stopCollecting();

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();

                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE, maxQueueSize);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, maxBatchSize);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, maxBatchLingerMs);

                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_OUT_SUCCESSFUL_KAFKA_TOPICS_CREATED, successfulCreations);

                performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, this.topicOperatorMetricsGatherer.getMetricsStore()); // Map of metrics history

                this.topicOperatorPerformanceReporter.logPerformanceData(this.testStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_CAPACITY_USE_CASE, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
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
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }

    @AfterAll
     void tearDown() {
        TopicOperatorPerformanceUtils.stopExecutor();
        // show tables with metrics
        TopicOperatorMetricsParser.main(new String[]{PerformanceConstants.TOPIC_OPERATOR_PARSER});
    }
}

