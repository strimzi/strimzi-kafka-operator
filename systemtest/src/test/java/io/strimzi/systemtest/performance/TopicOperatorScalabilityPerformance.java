/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.performance.report.TopicOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.TopicOperatorMetricsParser;
import io.strimzi.systemtest.performance.utils.TopicOperatorPerformanceUtils;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class TopicOperatorScalabilityPerformance extends AbstractST {

    protected static final TemporalAccessor ACTUAL_TIME = LocalDateTime.now();
    protected static final String REPORT_DIRECTORY = "topic-operator";

    protected TopicOperatorPerformanceReporter topicOperatorPerformanceReporter = new TopicOperatorPerformanceReporter();

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorScalabilityPerformance.class);

    private TestStorage suiteTestStorage;

    // topic event batches to test
    private final List<Integer> eventBatches = List.of(10, 100, 500, 1000);
    private final int maxBatchSize = 100;
    private final int maxBatchLingerMs = 100;
    private final int maxQueueSize = Integer.MAX_VALUE;
    private long reconciliationTimeMs;

    @IsolatedTest
    void testScalability() {
        eventBatches.forEach(numEvents -> {
            final int eventPerTask = 4;
            final int numberOfTasks = numEvents / eventPerTask;
            final int numSpareEvents = numEvents % eventPerTask;
            try {
                this.reconciliationTimeMs = TopicOperatorPerformanceUtils.processAllTopicsConcurrently(suiteTestStorage, numberOfTasks, numSpareEvents, 0);
            } finally {
                // safe net if something went wrong during test case and KafkaTopic is not properly deleted
                LOGGER.info("Cleaning namespace: {}", suiteTestStorage.getNamespaceName());
                resourceManager.deleteResourcesOfTypeWithoutWait(KafkaTopic.RESOURCE_KIND);
                KafkaTopicUtils.waitForTopicWithPrefixDeletion(suiteTestStorage.getNamespaceName(), suiteTestStorage.getTopicName());

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();

                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_QUEUE_SIZE, maxQueueSize);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_SIZE, maxBatchSize);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_TOPICS, numberOfTasks);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_NUMBER_OF_EVENTS, (numberOfTasks * 3) + numSpareEvents);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_MAX_BATCH_LINGER_MS, maxBatchLingerMs);
                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_IN_PROCESS_TYPE, "TOPIC-CONCURRENT");

                performanceAttributes.put(PerformanceConstants.TOPIC_OPERATOR_OUT_RECONCILIATION_INTERVAL, reconciliationTimeMs);

                try {
                    this.topicOperatorPerformanceReporter.logPerformanceData(this.suiteTestStorage, performanceAttributes, REPORT_DIRECTORY + "/" + PerformanceConstants.GENERAL_SCALABILITY_USE_CASE, ACTUAL_TIME, Environment.PERFORMANCE_DIR);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @BeforeAll
    void setUp() {
        setupClusterOperator
            .withDefaultConfiguration()
            .install();

        suiteTestStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
        );

        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(),  suiteTestStorage.getClusterName(), 3)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .editSpec()
                    .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("500Mi"))
                        .addToLimits("cpu", new Quantity("500m"))
                        .addToRequests("memory", new Quantity("500Mi"))
                        .addToRequests("cpu", new Quantity("500m"))
                        .build())
                    .endKafka()
                        .editEntityOperator()
                            .editTopicOperator()
                                .withReconciliationIntervalMs(10_000L)
                                .withResources(new ResourceRequirementsBuilder()
                                    .addToLimits("memory", new Quantity("500Mi"))
                                    .addToLimits("cpu", new Quantity("500m"))
                                    .addToRequests("memory", new Quantity("500Mi"))
                                    .addToRequests("cpu", new Quantity("500m"))
                                    .build())
                            .endTopicOperator()
                            .editOrNewTemplate()
                                .editOrNewTopicOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_USE_FINALIZERS")
                                        .withValue("false")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_MAX_BATCH_SIZE")
                                        .withValue(String.valueOf(maxBatchSize))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MAX_BATCH_LINGER_MS")
                                        .withValue(String.valueOf(maxBatchLingerMs))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("STRIMZI_MAX_QUEUE_SIZE")
                                        .withValue(String.valueOf(maxQueueSize))
                                    .endEnv()
                                .endTopicOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                    .endSpec()
                .build()
        );
    }

    @AfterAll
    void tearDown() {
        TopicOperatorPerformanceUtils.stopExecutor();
        // show tables with metrics
        TopicOperatorMetricsParser.main(new String[]{PerformanceConstants.TOPIC_OPERATOR_PARSER});
    }
}
