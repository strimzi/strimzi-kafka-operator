/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicSpec;
import io.strimzi.api.kafka.model.topic.KafkaTopicSpecBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

/**
 * Utility class for asynchronous batch processing of Kafka topic operations to measure
 * and optimize the performance of topic creation, modification, and deletion within a Kubernetes cluster.
 */
public class TopicOperatorPerformanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorPerformanceUtils.class);
    private static final Map<String, Object> KAFKA_TOPIC_CONFIG_TO_MODIFY = Map.of(
        "compression.type", "gzip", "cleanup.policy", "delete", "min.insync.replicas", 2,
        "max.compaction.lag.ms", 54321L, "min.compaction.lag.ms", 54L, "retention.ms", 3690L,
        "segment.ms", 123456L, "retention.bytes", 9876543L, "segment.bytes", 321654L, "flush.messages", 456123L);
    private static final int AVAILABLE_CPUS = Runtime.getRuntime().availableProcessors();

    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(AVAILABLE_CPUS);

    private TopicOperatorPerformanceUtils() {}  // Prevent instantiation

    /**
     * Processes Kafka topic operations across multiple batches asynchronously.
     * This method sets up a fixed thread pool based on the available CPU processors
     * and manages batch operations for creation, modification, and deletion of Kafka topics.
     *
     * Illustration of Concurrent Batch Processing:
     *
     * ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
     * │                                           Thread Pool                                            │
     * │                                                                                                  │
     * │   +------------------+    +------------------+    +------------------+    +------------------+   │
     * │   | Thread 1         |    | Thread 2         |    | Thread 3         |    | Thread N         |   │
     * │   | +--------------+ |    | +--------------+ |    | +--------------+ |    | +--------------+ |   │
     * │   | | Batch 1      | |    | | Batch 2      | |    | | Batch 3      | |    | | Batch N      | |   │
     * │   | |              | |    | |              | |    | |              | |    | |              | |   │
     * │   | | 1. Creation  | |    | | 1. Creation  | |    | | 1. Creation  | |    | | 1. Creation  | |   │
     * │   | | 2. Modific.  | | -->| | 2. Modific.  | | -->| | 2. Modific.  | | -->| | 2. Modific.  | |   │
     * │   | | 3. Deletion  | |    | | 3. Deletion  | |    | | 3. Deletion  | |    | | 3. Deletion  | |   │
     * │   | +--------------+ |    | +--------------+ |    | +--------------+ |    | +--------------+ |   │
     * │   +------------------+    +------------------+    +------------------+    +------------------+   │
     * │                                                                                                  │
     * │ - Each thread processes a specific batch, performing creation, modification, and deletion        │
     * │   sequentially, but each batch operation is independent and concurrent across threads.           │
     * │ - Threads are managed by an executor service that efficiently schedules each batch               │
     * │   task to maximize the available CPU resources.                                                  │
     * │ - Errors in any batch operation are logged and do not impact the processing of other             │
     * │   batches, ensuring robust execution.                                                            │
     * └──────────────────────────────────────────────────────────────────────────────────────────────────┘
     *
     * @param testStorage    The storage instance containing the configuration and namespace details.
     * @param numberOfTopics The total number of Kafka topics to process.
     */
    public static void processKafkaTopicBatchesAsync(TestStorage testStorage, int numberOfTopics) {
        final int topicsPerBatch = (numberOfTopics + AVAILABLE_CPUS - 1) / AVAILABLE_CPUS; // Ensures all topics are covered

        final ExtensionContext currentContext = ResourceManager.getTestContext();
        final CompletableFuture<?>[] futures = new CompletableFuture[AVAILABLE_CPUS];

        for (int batch = 0; batch < AVAILABLE_CPUS; batch++) {
            int start = batch * topicsPerBatch;
            int end = Math.min(start + topicsPerBatch, numberOfTopics); // Ensure we do not go out of bounds
            // Delegate batch processing to a separate method
            futures[batch] = processBatch(start, end, currentContext, testStorage);
        }
        CompletableFuture.allOf(futures).join(); // Wait for all batches to complete
        LOGGER.info("All batches completed.");
    }

    /**
     * Manages the creation, modification, and deletion of Kafka topics for a specific batch.
     * Operations are performed sequentially within each batch but executed concurrently across multiple batches,
     * utilizing multiple threads to improve performance.
     *
     *
     * @param start           Starting index of the batch.
     * @param end             Ending index of the batch.
     * @param currentContext  The current JUnit extension context.
     * @param testStorage     Storage instance with namespace and configuration details.
     * @return                CompletableFuture representing the completion of all operations in the batch.
     */
    private static CompletableFuture<Void> processBatch(int start, int end, ExtensionContext currentContext,
                                                        TestStorage testStorage) {
        return CompletableFuture.runAsync(() -> performCreationWithWait(start, end, currentContext, testStorage), EXECUTOR)
            .thenRunAsync(() -> performModificationWithWait(start, end, currentContext, testStorage, KAFKA_TOPIC_CONFIG_TO_MODIFY), EXECUTOR)
            .thenRunAsync(() -> performDeletionWithWait(start, end, currentContext, testStorage), EXECUTOR)
            .exceptionally(ex -> {
                LOGGER.error("Error processing batch from {} to {}: {}", start, end, ex.getMessage(), ex);
                return null;
            });
    }

    /**
     * Creates Kafka topics within a specified range and waits until their status is ready.
     *
     * @param start             the starting index of the Kafka topics to create
     * @param end               the ending index of the Kafka topics to create
     * @param currentContext    the current test context
     * @param testStorage       storage containing test information such as namespace, cluster, and topic names
     *
     * <p>Note: The {@code ResourceManager.setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     */
    private static void performCreationWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        ResourceManager.setTestContext(currentContext);
        LOGGER.info("Creating Kafka topics from index {} to {}", start, end);
        KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(),
            testStorage.getTopicName(), start, end, 12, 3, 2);
        KafkaTopicScalabilityUtils.waitForTopicStatus(testStorage.getNamespaceName(), testStorage.getTopicName(),
            start, end, CustomResourceStatus.Ready, ConditionStatus.True);
    }

    /**
     * Modifies Kafka topics within a specified range and waits until their configuration is updated.
     *
     * @param start                      the starting index of the Kafka topics to modify
     * @param end                        the ending index of the Kafka topics to modify
     * @param currentContext             the current test context
     * @param testStorage                storage containing test information such as namespace and topic names
     * @param kafkaTopicConfigToModify   configuration to modify in the Kafka topics
     *
     * <p>Note: The {@code ResourceManager.setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     */
    private static void performModificationWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage, Map<String, Object> kafkaTopicConfigToModify) {
        ResourceManager.setTestContext(currentContext);
        LOGGER.info("Modifying Kafka topics from index {} to {}", start, end);
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(testStorage.getNamespaceName(), testStorage.getTopicName(),
            start, end, new KafkaTopicSpecBuilder().withConfig(kafkaTopicConfigToModify).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(testStorage.getNamespaceName(), testStorage.getTopicName(),
            start, end, kafkaTopicConfigToModify);
    }

    /**
     * Deletes Kafka topics within a specified range and waits until they are fully deleted.
     *
     * @param start         the starting index of the Kafka topics to delete
     * @param end           the ending index of the Kafka topics to delete
     * @param currentContext the current test context
     * @param testStorage   storage containing test information such as namespace and topic names
     *
     * <p>Note: The {@code ResourceManager.setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     */
    private static void performDeletionWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        ResourceManager.setTestContext(currentContext);
        LOGGER.info("Deleting Kafka topics from index {} to {}", start, end);
        KafkaTopicUtils.deleteKafkaTopicsInRange(testStorage.getNamespaceName(), testStorage.getTopicName(), start, end);
        KafkaTopicUtils.waitForTopicWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getTopicName(), start, end);
    }

    /**
     * Manages the full lifecycle of Kafka topics concurrently using a fixed thread pool.
     * This method processes creation, modification, and deletion for each topic in separate threads,
     * ensuring all topics are handled simultaneously across available CPU resources.
     *
     * +-----------------------------------------------------------------------------+
     * |                            processAllTopicsConcurrently                     |
     * +-----------------------------------------------------------------------------+
     * | +-------------+       +-------------+             +-------------+           |
     * | | Topic 1     |       | Topic 2     |     ...     | Topic N     |           |
     * | | +---------+ |       | +---------+ |             | +---------+ |           |
     * | | | Thread  | |       | | Thread  | |             | | Thread  | |           |
     * | | |         | |       | |         | |             | |         | |           |
     * | | | Creation| |       | | Creation| |             | | Creation| |           |
     * | | | Update  | |       | | Update  | |             | | Update  | |           |
     * | | | Deletion| |       | | Deletion| |             | | Deletion| |           |
     * | | +---------+ |       | +---------+ |             | +---------+ |           |
     * | +-------------+       +-------------+             +-------------+           |
     * +-----------------------------------------------------------------------------+
     *
     * @param testStorage           An instance of TestStorage containing configuration and state needed for topic operations.
     * @param numberOfTopics        The number of Kafka topics to be processed.
     * @param spareEvents           The number of spare events to be consumed during the process.
     * @param warmUpTasksToProcess  The number of tasks to warm-up performance and optimize JIT. This number is used just for offsetting.
     *
     * @return                      The total time taken to complete all topic lifecycles in milliseconds.
     */
    public static long processAllTopicsConcurrently(TestStorage testStorage, int numberOfTopics, int spareEvents, int warmUpTasksToProcess) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ExtensionContext extensionContext = ResourceManager.getTestContext();

        long startTime = System.nanoTime();

        for (int topicIndex = warmUpTasksToProcess; topicIndex < numberOfTopics + warmUpTasksToProcess; topicIndex++) {
            final int finalTopicIndex = topicIndex;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> performFullLifecycle(finalTopicIndex, testStorage, extensionContext), EXECUTOR);
            futures.add(future);
        }

        // consume spare events
        for (int j = 0; j < spareEvents; j++) {
            futures.add(j, CompletableFuture.completedFuture(null));
        }

        // Wait for all topics to complete their lifecycle
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.info("All topic lifecycles completed.");

        long allTasksTimeMs = (System.nanoTime() - startTime) / 1_000_000;

        if (warmUpTasksToProcess != 0) {
            // boundary between tests => less likelihood that tests would influence each other
            LOGGER.info("Cooling down");
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return allTasksTimeMs;
    }

    public static void stopExecutor() {
        try {
            EXECUTOR.shutdown();
            EXECUTOR.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            if (!EXECUTOR.isTerminated()) {
                EXECUTOR.shutdownNow();
            }
        }
    }

    /**
     * Executes the full lifecycle of Topic Operator tasks which includes creation, modification,
     * and deletion operations. These operations are encapsulated as a single task that is suitable
     * for parallel processing.
     *
     * @param topicIndex        the index of the topic which identifies the specific topic to be managed.
     * @param testStorage       an object representing the storage where test data or states are maintained.
     * @param extensionContext  an object representing current context of the test case
     */
    private static void performFullLifecycle(int topicIndex, TestStorage testStorage, ExtensionContext extensionContext) {
        performCreationWithWait(topicIndex, topicIndex + 1, extensionContext, testStorage);
        performModificationWithWait(topicIndex, topicIndex + 1, extensionContext, testStorage, KAFKA_TOPIC_CONFIG_TO_MODIFY);
        performDeletionWithWait(topicIndex, topicIndex + 1, extensionContext, testStorage);
    }

    /**
     * Creates a KafkaTopic and waits until their status is ready.
     *
     * <p>Note: The {@code ResourceManager.setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     *
     * @param namespace          The namespace name.
     * @param clusterName        The cluster name.
     * @param topicName          The topic name.
     * @param currentContext     The current context of the test case.
     */
    public static void createTopicWithWait(String namespace, String clusterName, String topicName, ExtensionContext currentContext) {
        ResourceManager.setTestContext(currentContext);
        createTopic(namespace, clusterName, topicName, 12, 3, 2);
        waitForTopicStatus(namespace, topicName, CustomResourceStatus.Ready, ConditionStatus.True);
    }

    /**
     * Updates a KafkaTopic and waits until the configuration is updated.
     *
     * <p>Note: The {@code ResourceManager.setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     *
     * @param namespace          The namespace name
     * @param clusterName        The cluster name.
     * @param topicName          The topic name.
     * @param currentContext     The current context of the test case.
     * @param configToUpdate     Configuration to update in the Kafka topics.
     */
    public static void updateTopicWithWait(String namespace, String clusterName, String topicName, ExtensionContext currentContext, Map<String, Object> configToUpdate) {
        ResourceManager.setTestContext(currentContext);
        updateTopic(namespace, topicName, new KafkaTopicSpecBuilder().withConfig(configToUpdate).build());
        waitForTopicConfigContains(namespace, topicName, configToUpdate);
    }

    /**
     * Deletes Kafka topics within a specified range and waits until they are fully deleted.
     *
     * <p>Note: The {@code ResourceManager.setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     *
     * @param namespace          The namespace name.
     * @param topicName          The topic name.
     * @param currentContext     The current context of the test case.
     */
    public static void deleteTopicWithWait(String namespace, String topicName, ExtensionContext currentContext) {
        ResourceManager.setTestContext(currentContext);
        deleteKafkaTopic(namespace, topicName);
        waitForKafkaTopicDeletion(namespace, topicName);
    }

    private static void createTopic(String namespace, String clusterName, String topicName, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.debug("Creating KafkaTopic {}/{}", namespace, topicName);
        ResourceManager.getInstance().createResourceWithoutWait(KafkaTopicTemplates.topic(
                namespace, clusterName, topicName, numberOfPartitions, numberOfReplicas, minInSyncReplicas).build());
    }

    private static void waitForTopicStatus(String namespace, String topicName, Enum<?> conditionType, ConditionStatus conditionStatus) {
        LOGGER.debug("Waiting for KafkaTopic {}/{} to be {}", namespace, topicName, conditionType.toString());
        KafkaTopicUtils.waitForKafkaTopicStatus(namespace, topicName, conditionType, conditionStatus);
    }

    private static void updateTopic(String namespace, String topicName, KafkaTopicSpec topicSpec) {
        LOGGER.debug("Updating KafkaTopic {}/{}", namespace, topicName);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.setSpec(topicSpec), namespace);
    }

    public static void waitForTopicConfigContains(String namespace, String topicName, Map<String, Object> config) {
        LOGGER.debug("Waiting for KafkaTopic: {}/{} to contain correct config", namespace, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespace + "/" + topicName + " to contain correct config",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
                () -> KafkaTopicUtils.configsAreEqual(KafkaTopicResource.kafkaTopicClient()
                        .inNamespace(namespace).withName(topicName).get().getSpec().getConfig(), config)
        );
        LOGGER.debug("KafkaTopic: {}/{} contains correct config", namespace, topicName);
    }

    private static void deleteKafkaTopic(String namespace, String topicName) {
        LOGGER.debug("Deleting topic {}", topicName);
        ResourceManager.cmdKubeClient().namespace(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
    }

    public static void waitForKafkaTopicDeletion(String namespaceName, String topicName) {
        LOGGER.debug("Waiting for KafkaTopic: {}/{} deletion", namespaceName, topicName);
        TestUtils.waitFor("deletion of KafkaTopic: " + namespaceName + "/" + topicName,
                TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                TestConstants.GLOBAL_TIMEOUT_SHORT,
                () -> {
                    if (KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get() == null) {
                        return true;
                    } else {
                        LOGGER.warn("KafkaTopic: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespaceName, topicName);
                        cmdKubeClient(namespaceName).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                        return false;
                    }
                },
                () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }
}
