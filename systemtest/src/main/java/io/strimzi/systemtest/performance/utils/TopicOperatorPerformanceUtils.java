/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicSpecBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
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
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;

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
        final int availableCPUs = Runtime.getRuntime().availableProcessors();
        final int optimalBatchCount = Math.max(1, availableCPUs);
        final int topicsPerBatch = (numberOfTopics + optimalBatchCount - 1) / optimalBatchCount; // Ensures all topics are covered

        final ExecutorService executor = Executors.newFixedThreadPool(optimalBatchCount);
        final ExtensionContext currentContext = ResourceManager.getTestContext();
        final CompletableFuture<?>[] futures = new CompletableFuture[optimalBatchCount];

        try {
            for (int batch = 0; batch < optimalBatchCount; batch++) {
                int start = batch * topicsPerBatch;
                int end = Math.min(start + topicsPerBatch, numberOfTopics); // Ensure we do not go out of bounds
                // Delegate batch processing to a separate method
                futures[batch] = processBatch(start, end, currentContext, testStorage, executor);
            }
            CompletableFuture.allOf(futures).join(); // Wait for all batches to complete
            LOGGER.info("All batches completed.");
        } finally {
            executor.shutdown();
        }
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
     * @param executor        The executor service managing concurrent tasks.
     * @return                CompletableFuture representing the completion of all operations in the batch.
     */
    private static CompletableFuture<Void> processBatch(int start, int end, ExtensionContext currentContext,
                                                        TestStorage testStorage, ExecutorService executor) {
        return CompletableFuture.runAsync(() -> performCreationWithWait(start, end, currentContext, testStorage), executor)
            .thenRunAsync(() -> performModificationWithWait(start, end, currentContext, testStorage, KAFKA_TOPIC_CONFIG_TO_MODIFY), executor)
            .thenRunAsync(() -> performDeletionWithWait(start, end, currentContext, testStorage), executor)
            .exceptionally(ex -> {
                LOGGER.error("Error processing batch from {} to {}: {}", start, end, ex.getMessage(), ex);
                return null;
            });
    }

    private static void performCreationWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        ResourceManager.setTestContext(currentContext);
        LOGGER.info("Creating Kafka topics from index {} to {}", start, end);
        KafkaTopicScalabilityUtils.createTopicsViaK8s(testStorage.getNamespaceName(), testStorage.getClusterName(),
            testStorage.getTopicName(), start, end, 12, 3, 2);
        KafkaTopicScalabilityUtils.waitForTopicStatus(testStorage.getNamespaceName(), testStorage.getTopicName(),
            start, end, CustomResourceStatus.Ready, ConditionStatus.True);
    }

    private static void performModificationWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage, Map<String, Object> kafkaTopicConfigToModify) {
        ResourceManager.setTestContext(currentContext);
        LOGGER.info("Modifying Kafka topics from index {} to {}", start, end);
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(testStorage.getNamespaceName(), testStorage.getTopicName(),
            start, end, new KafkaTopicSpecBuilder().withConfig(kafkaTopicConfigToModify).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(testStorage.getNamespaceName(), testStorage.getTopicName(),
            start, end, kafkaTopicConfigToModify);
    }

    private static void performDeletionWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        ResourceManager.setTestContext(currentContext);
        LOGGER.info("Deleting Kafka topics from index {} to {}", start, end);
        deleteKafkaTopicsInRange(testStorage.getNamespaceName(), testStorage.getTopicName(), start, end);
        waitForTopicWithPrefixDeletion(testStorage.getNamespaceName(), testStorage.getTopicName(), start, end);
    }

    /**
     * Deletes Kafka topics within a specified range based on their names.
     * Assumes topic names are structured to include a numerical suffix indicating their batch.
     *
     * @param namespace     the Kubernetes namespace
     * @param prefix        the common prefix of the Kafka topic names
     * @param start         the starting index of topics to delete
     * @param end           the ending index of topics to delete
     */
    public static void deleteKafkaTopicsInRange(String namespace, String prefix, int start, int end) {
        List<KafkaTopic> topicsToDelete = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems()
            .stream()
            .filter(topic -> {
                String name = topic.getMetadata().getName();
                if (!name.startsWith(prefix)) return false;
                // Extract index from the topic name
                String indexPart = name.substring(prefix.length());
                try {
                    int index = Integer.parseInt(indexPart);
                    return index >= start && index < end;
                } catch (NumberFormatException e) {
                    LOGGER.error("Failed to parse index from Kafka topic name: {}", name, e);
                    return false;
                }
            }).toList();

        topicsToDelete.forEach(topic ->
            cmdKubeClient().namespace(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topic.getMetadata().getName())
        );

        LOGGER.info("Deleted Kafka topics from {} to {} in namespace {} with prefix {}", start, end, namespace, prefix);
    }

    public static List<KafkaTopic> getAllKafkaTopicsWithPrefix(String namespace, String prefix, int start, int end) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems()
            .stream()
            .filter(topic -> {
                String name = topic.getMetadata().getName();
                if (!name.startsWith(prefix)) return false;
                String indexPart = name.substring(prefix.length());
                try {
                    int index = Integer.parseInt(indexPart);
                    return index >= start && index < end;
                } catch (NumberFormatException e) {
                    LOGGER.error("Failed to parse index from Kafka topic name: {}", name, e);
                    return false;
                }
            })
            .collect(Collectors.toList());
    }

    public static void waitForTopicWithPrefixDeletion(String namespaceName, String topicPrefix, int start, int end) {
        TestUtils.waitFor("deletion of all topics with prefix: " + topicPrefix + " from " + start + " to " + end,
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    final int numberOfTopicsToDelete = getAllKafkaTopicsWithPrefix(namespaceName, topicPrefix, start, end).size();
                    LOGGER.info("Remaining KafkaTopic's to delete from {} to {}: {} !", start, end, numberOfTopicsToDelete);
                    return numberOfTopicsToDelete == 0;
                } catch (Exception e) {
                    LOGGER.error("Error while checking for topic deletion: ", e);
                    return e.getMessage().contains("Not Found") || e.getMessage().contains("the server doesn't have a resource type");
                }
            });
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
     * @param testStorage       An instance of TestStorage containing configuration and state needed for topic operations.
     * @param numberOfTopics    The number of Kafka topics to be processed.
     */
    public static void processAllTopicsConcurrently(TestStorage testStorage, int numberOfTopics) {
        final int availableCPUs = Math.max(1, Runtime.getRuntime().availableProcessors());
        final ExecutorService executor = Executors.newFixedThreadPool(availableCPUs);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ExtensionContext extensionContext = ResourceManager.getTestContext();

        for (int topicIndex = 0; topicIndex < numberOfTopics; topicIndex++) {
            final int finalTopicIndex = topicIndex;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> performFullLifecycle(finalTopicIndex, testStorage, extensionContext), executor);
            futures.add(future);
        }

        // Wait for all topics to complete their lifecycle
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.info("All topic lifecycles completed.");

        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
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
}
