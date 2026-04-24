/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Utility class for managing KafkaUser resources in performance testing scenarios. It provides methods to create,
 * alter, and monitor KafkaUsers efficiently, supporting performance evaluations of the User Operator.
 */
public class UserOperatorPerformanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorPerformanceUtils.class);

    // ensuring that object can not be created outside of class
    private UserOperatorPerformanceUtils() {}

    public static void alterAllUsersInList(final TestStorage testStorage, final List<KafkaUser> listOfUsers, final String usersPrefix) {
        LOGGER.info("Altering {} KafkaUsers", listOfUsers.size());

        KafkaUserQuotas kafkaUserQuotas = new KafkaUserQuotasBuilder()
            .withConsumerByteRate(1000)
            .withProducerByteRate(2000)
            .withRequestPercentage(42)
            .withControllerMutationRate(10d)
            .build();

        KafkaUserAuthorizationSimple updatedAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                .withName(testStorage.getTopicName())
            .endAclRuleTopicResource()
            .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        for (KafkaUser kafkaUser : listOfUsers) {
            KafkaUserUtils.replace(testStorage.getNamespaceName(), kafkaUser.getMetadata().getName(), ku -> {
                ku.getSpec().setAuthorization(updatedAcl);
                ku.getSpec().setQuotas(kafkaUserQuotas);
            });
        }

        // Wait for each specific user in the list to be updated and ready
        listOfUsers.forEach(kafkaUser ->
            KafkaUserUtils.waitForKafkaUserReady(testStorage.getNamespaceName(), kafkaUser.getMetadata().getName())
        );
    }

    public static List<KafkaUser> getListOfKafkaUsers(final TestStorage testStorage, final String userName,
                                                      final int numberOfUsers, final UserAuthType userAuthType) {
        return getListOfKafkaUsers(testStorage, userName, 0, numberOfUsers, userAuthType);
    }

    public static List<KafkaUser> getListOfKafkaUsers(final TestStorage testStorage, final String userName,
                                                      final int startPointer, final int endPointer, final UserAuthType userAuthType) {
        List<KafkaUser> usersList = new ArrayList<>();

        KafkaUserAuthorizationSimple usersAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                    .withName(testStorage.getTopicName())
                .endAclRuleTopicResource()
                .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        // Loop over the specific range from startPointer to endPointer
        for (int i = startPointer; i < endPointer; i++) {
            if (userAuthType.equals(UserAuthType.Tls)) {
                usersList.add(
                    KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), userName + "-" + i, testStorage.getClusterName())
                        .editOrNewSpec()
                            .withAuthorization(usersAcl)
                        .endSpec()
                        .build()
                );
            } else {
                usersList.add(
                    KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), userName + "-" + i, testStorage.getClusterName())
                        .editOrNewSpec()
                            .withAuthorization(usersAcl)
                        .endSpec()
                        .build()
                );
            }
        }

        return usersList;
    }

    public static void createAllUsersInListWithWait(final TestStorage testStorage, final List<KafkaUser> listOfUsers, final String usersPrefix) {
        LOGGER.info("Creating {} KafkaUsers", listOfUsers.size());

        KubeResourceManager.get().createResourceWithoutWait(listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));

        // Wait for each specific user in the list to be ready
        listOfUsers.forEach(kafkaUser ->
            KafkaUserUtils.waitForKafkaUserReady(testStorage.getNamespaceName(), kafkaUser.getMetadata().getName())
        );
    }

    public static void cleanupKafkaUsers(TestStorage testStorage) {
        LOGGER.info("Cleaning namespace: {}", testStorage.getNamespaceName());

        List<KafkaUser> kafkaUsers = CrdClients.kafkaUserClient()
            .inNamespace(testStorage.getNamespaceName()).list().getItems()
            .stream()
            .filter(u -> u.getMetadata().getName().startsWith(testStorage.getUsername()))
            .toList();

        if (kafkaUsers.isEmpty()) {
            LOGGER.info("No KafkaUsers with prefix '{}' found in namespace {}",
                testStorage.getUsername(), testStorage.getNamespaceName());
            return;
        }

        final int batchSize = 500;
        final int totalBatches = (kafkaUsers.size() + batchSize - 1) / batchSize;
        LOGGER.info("Deleting {} KafkaUsers in {} batch(es) of up to {}",
            kafkaUsers.size(), totalBatches, batchSize);

        final long cleanupStartTime = System.nanoTime();

        for (int i = 0; i < kafkaUsers.size(); i += batchSize) {
            final int batchNumber = (i / batchSize) + 1;
            List<KafkaUser> batch = kafkaUsers.subList(i, Math.min(i + batchSize, kafkaUsers.size()));
            final int expectedRemaining = kafkaUsers.size() - (i + batch.size());

            LOGGER.info("Submitting deletion batch {}/{} ({} users, expecting {} remaining after)",
                batchNumber, totalBatches, batch.size(), expectedRemaining);

            final long batchStartTime = System.nanoTime();
            KubeResourceManager.get().deleteResourceWithoutWait(batch.toArray(new KafkaUser[0]));
            final long deleteApiCallMs = Duration.ofNanos(System.nanoTime() - batchStartTime).toMillis();
            LOGGER.info("Batch {}/{} delete API calls took {} ms", batchNumber, totalBatches, deleteApiCallMs);

            List<String> batchSecretNames = batch.stream()
                .map(u -> u.getMetadata().getName())
                .toList();

            TestUtils.waitFor(
                "deletion of batch " + batchNumber + "/" + totalBatches,
                TestConstants.GLOBAL_POLL_INTERVAL,
                TestConstants.GLOBAL_TIMEOUT,
                () -> {
                    try {
                        final int remaining = KafkaUserUtils.getAllKafkaUsersWithPrefix(
                            testStorage.getNamespaceName(), testStorage.getUsername()).size();
                        LOGGER.debug("Waiting for batch {}/{} deletion — {} users remaining (expecting <= {})",
                            batchNumber, totalBatches, remaining, expectedRemaining);
                        return remaining <= expectedRemaining;
                    } catch (Exception e) {
                        return e.getMessage().contains("Not Found")
                            || e.getMessage().contains("the server doesn't have a resource type");
                    }
                });

            waitForUserSecretsCleanup(testStorage, batchSecretNames, batchNumber, totalBatches);

            final long batchTotalMs = Duration.ofNanos(System.nanoTime() - batchStartTime).toMillis();
            LOGGER.info("Batch {}/{} completed in {} ms (API calls: {} ms, reconciliation wait: {} ms)",
                batchNumber, totalBatches, batchTotalMs, deleteApiCallMs, batchTotalMs - deleteApiCallMs);
        }

        final long totalCleanupMs = Duration.ofNanos(System.nanoTime() - cleanupStartTime).toMillis();
        LOGGER.info("Full cleanup (KafkaUsers + Secrets) of {} users completed in {} ms ({} ms/user avg)",
            kafkaUsers.size(), totalCleanupMs,
            kafkaUsers.size() > 0 ? totalCleanupMs / kafkaUsers.size() : 0);
    }

    /**
     * Waits until all Secrets for the given batch of deleted KafkaUsers are fully removed.
     * The User Operator deletes Secrets one-by-one through its reconciliation loop, which
     * can be slow for large numbers of users. This method ensures each batch's Secrets are
     * cleaned up before proceeding to the next batch.
     *
     * @param testStorage       storage containing namespace information
     * @param secretNames       names of the Secrets to wait for (same as the KafkaUser names)
     * @param batchNumber       current batch number (for logging)
     * @param totalBatches      total number of batches (for logging)
     */
    private static void waitForUserSecretsCleanup(TestStorage testStorage,
                                                  List<String> secretNames,
                                                  int batchNumber,
                                                  int totalBatches) {
        final Set<String> secretNameSet = Set.copyOf(secretNames);

        LOGGER.info("Waiting for {} Secrets from batch {}/{} to be cleaned up",
            secretNameSet.size(), batchNumber, totalBatches);

        final long secretWaitStart = System.nanoTime();

        TestUtils.waitFor(
            "deletion of Secrets from batch " + batchNumber + "/" + totalBatches,
            TestConstants.GLOBAL_POLL_INTERVAL,
            TestConstants.GLOBAL_TIMEOUT,
            () -> {
                List<String> remaining = KubeResourceManager.get().kubeClient().getClient()
                    .secrets()
                    .inNamespace(testStorage.getNamespaceName())
                    .withLabel(Labels.STRIMZI_KIND_LABEL, KafkaUser.RESOURCE_KIND)
                    .list()
                    .getItems()
                    .stream()
                    .map(s -> s.getMetadata().getName())
                    .filter(secretNameSet::contains)
                    .toList();

                if (remaining.isEmpty()) {
                    return true;
                }

                LOGGER.debug("Batch {}/{}: still {} Secrets remaining",
                    batchNumber, totalBatches, remaining.size());
                return false;
            });

        final long secretWaitMs = Duration.ofNanos(System.nanoTime() - secretWaitStart).toMillis();
        LOGGER.info("Batch {}/{} Secrets cleaned up in {} ms",
            batchNumber, totalBatches, secretWaitMs);
    }

    /**
     * Manages the full lifecycle of Kafka users concurrently using a fixed thread pool.
     * This method processes creation, modification, and deletion for each user in separate threads,
     * allowing concurrent operations which is optimal for I/O-bound Kubernetes API calls.
     *
     * @param testStorage           An instance of TestStorage containing configuration and state needed for user operations.
     * @param numberOfUsers         The number of Kafka users to be processed.
     * @param spareEvents           The number of spare events to be consumed during the process.
     * @param warmUpTasksToProcess  The number of tasks to warm-up performance and optimize JIT. This number is used just for offsetting.
     *
     * @return                      The total time taken to complete all user lifecycles in milliseconds.
     */
    public static long processAllUsersConcurrently(TestStorage testStorage, int numberOfUsers, int spareEvents, int warmUpTasksToProcess) {
        return PerformanceTestExecutorService.processResourcesConcurrently(
            numberOfUsers,
            spareEvents,
            warmUpTasksToProcess,
            (userIndex, extensionContext) -> performFullLifecycle(userIndex, testStorage, extensionContext),
            KafkaUser.RESOURCE_KIND
        );
    }

    /**
     * Stops the shared executor service gracefully.
     */
    public static void stopExecutor() {
        PerformanceTestExecutorService.stopExecutor();
    }

    /**
     * Executes the full lifecycle of User Operator tasks which includes creation, modification,
     * and deletion operations. These operations are encapsulated as a single task that is suitable
     * for parallel processing.
     *
     * @param startIndex        an index of users to be managed (inclusive).
     * @param testStorage       an object representing the storage where test data or states are maintained.
     * @param extensionContext  an object representing current context of the test case
     */
    private static void performFullLifecycle(int startIndex, TestStorage testStorage, ExtensionContext extensionContext) {
        performCreationWithWait(startIndex, startIndex + 1, extensionContext, testStorage);
        performModificationWithWait(startIndex, startIndex + 1, extensionContext, testStorage);
        performDeletionWithWait(startIndex, startIndex + 1, extensionContext, testStorage);
    }

    /**
     * Creates Kafka users within a specified range and waits until their status is ready.
     *
     * @param start             the starting index of the Kafka users to create
     * @param end               the ending index of the Kafka users to create
     * @param currentContext    the current test context
     * @param testStorage       storage containing test information such as namespace, cluster, and user names
     *
     * <p>Note: The {@code KubeResourceManager.get().setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     */
    private static void performCreationWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        KubeResourceManager.get().setTestContext(currentContext);
        LOGGER.info("Creating Kafka users from index {} to {}", start, end);
        List<KafkaUser> users = getListOfKafkaUsers(testStorage, testStorage.getUsername(), start, end, UserAuthType.Tls);
        createAllUsersInListWithWait(testStorage, users, testStorage.getUsername());
    }

    /**
     * Modifies Kafka users within a specified range and waits until their configuration is updated.
     *
     * @param start                      the starting index of the Kafka users to modify
     * @param end                        the ending index of the Kafka users to modify
     * @param currentContext             the current test context
     * @param testStorage                storage containing test information such as namespace and user names
     *
     * <p>Note: The {@code KubeResourceManager.get().setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     */
    private static void performModificationWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        KubeResourceManager.get().setTestContext(currentContext);
        LOGGER.info("Modifying Kafka users from index {} to {}", start, end);
        List<KafkaUser> users = getListOfKafkaUsers(testStorage, testStorage.getUsername(), start, end, UserAuthType.Tls);
        alterAllUsersInList(testStorage, users, testStorage.getUsername());
    }

    /**
     * Deletes Kafka users within a specified range and waits until they are fully deleted.
     *
     * @param start         the starting index of the Kafka users to delete
     * @param end           the ending index of the Kafka users to delete
     * @param currentContext the current test context
     * @param testStorage   storage containing test information such as namespace and user names
     *
     * <p>Note: The {@code KubeResourceManager.get().setTestContext(currentContext);} is needed because this method is invoked in a new thread.
     * Therefore, if you do not set the context, you would end up with a NullPointerException (NPE) because a new thread does not hold
     * the state of the {@code ExtensionContext}, and so you need to set it.</p>
     */
    private static void performDeletionWithWait(int start, int end, ExtensionContext currentContext, TestStorage testStorage) {
        KubeResourceManager.get().setTestContext(currentContext);
        LOGGER.info("Deleting Kafka users from index {} to {}", start, end);
        List<KafkaUser> kafkaUsers = CrdClients.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).list().getItems()
            .stream()
            .filter(user -> {
                String userName = user.getMetadata().getName();
                int index = extractUserIndex(userName, testStorage.getUsername());
                return index >= start && index < end;
            })
            .toList();
        KubeResourceManager.get().deleteResourceAsyncWait(kafkaUsers.toArray(new KafkaUser[0]));
        // Wait for deletion of only the users in the range [start, end) instead of all users with the prefix
        kafkaUsers.forEach(user ->
            KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), user.getMetadata().getName())
        );
    }

    /**
     * Measures the latency of a single user modification operation under load.
     * This method first creates N users to establish load, then measures how long it takes to modify ONE additional user.
     * This simulates real-world scenario: "How responsive is the User Operator when there are already X users in the system?"
     *
     * @param testStorage           An instance of TestStorage containing configuration and state needed for user operations.
     * @param numberOfExistingUsers The number of existing Kafka users to create (establishing the load level).
     * @param numberOfModifications The number of single-user modifications to perform (for statistical averaging).
     *
     * @return                      LatencyMetrics containing statistical data about single operation latencies under load.
     */
    public static LatencyMetrics measureLatencyUnderLoad(final TestStorage testStorage,
                                                         final int numberOfExistingUsers,
                                                         final int numberOfModifications) {
        // Step 1: Create N users to establish load
        LOGGER.info("Creating {} users to establish load...", numberOfExistingUsers);
        final List<KafkaUser> existingUsers = getListOfKafkaUsers(testStorage, testStorage.getUsername(), 0, numberOfExistingUsers, UserAuthType.Tls);
        createAllUsersInListWithWait(testStorage, existingUsers, testStorage.getUsername());
        LOGGER.info("Load established with {} users", numberOfExistingUsers);

        // Step 2: Measure latency of individual modifications under this load
        final Map<Integer, Long> latencies = new ConcurrentHashMap<>();

        for (int i = 0; i < numberOfModifications; i++) {
            final int userIndexToModify = i % numberOfExistingUsers;
            final KafkaUser userToModify = existingUsers.get(userIndexToModify);

            LOGGER.info("Measuring modification latency for user {} ({}/{})", userToModify.getMetadata().getName(), i + 1, numberOfModifications);

            // Measure time to modify this single user
            final long startTime = System.nanoTime();

            final KafkaUserQuotas kafkaUserQuotas = new KafkaUserQuotasBuilder()
                .withConsumerByteRate(1000 + i * 100) // Vary quotas to ensure actual modification
                .withProducerByteRate(2000 + i * 100)
                .withRequestPercentage(42)
                .withControllerMutationRate(10d)
                .build();

            final KafkaUserAuthorizationSimple updatedAcl = new KafkaUserAuthorizationSimpleBuilder()
                .addNewAcl()
                    .withNewAclRuleTopicResource()
                        .withName(testStorage.getTopicName())
                    .endAclRuleTopicResource()
                    .withOperations(AclOperation.READ, AclOperation.DESCRIBE, AclOperation.WRITE)
                .endAcl()
                .build();

            KafkaUserUtils.replace(testStorage.getNamespaceName(), userToModify.getMetadata().getName(), ku -> {
                ku.getSpec().setAuthorization(updatedAcl);
                ku.getSpec().setQuotas(kafkaUserQuotas);
            });
            KafkaUserUtils.waitForKafkaUserReady(testStorage.getNamespaceName(), userToModify.getMetadata().getName());

            final long latencyMs = Duration.ofNanos(System.nanoTime() - startTime).toMillis();
            latencies.put(i, latencyMs);

            LOGGER.info("Modification latency: {} ms", latencyMs);
        }

        LOGGER.info("Completed {} modifications under load of {} existing users", numberOfModifications, numberOfExistingUsers);
        return calculateLatencyMetrics(latencies);
    }

    /**
     * Calculates latency statistics from a map of individual latencies.
     *
     * @param latencies Map containing individual latency measurements in milliseconds
     * @return LatencyMetrics object containing statistical analysis
     */
    private static LatencyMetrics calculateLatencyMetrics(final Map<Integer, Long> latencies) {
        if (latencies.isEmpty()) {
            return new LatencyMetrics(0, 0, 0, 0, 0, 0);
        }

        final List<Long> sortedLatencies = latencies.values().stream()
            .sorted()
            .collect(Collectors.toList());

        final long min = sortedLatencies.get(0);
        final long max = sortedLatencies.get(sortedLatencies.size() - 1);
        final double average = sortedLatencies.stream()
            .mapToLong(Long::longValue)
            .average()
            .orElse(0.0);

        final long p50 = calculatePercentile(sortedLatencies, 50);
        final long p95 = calculatePercentile(sortedLatencies, 95);
        final long p99 = calculatePercentile(sortedLatencies, 99);

        LOGGER.info("Latency Statistics - Min: {} ms, Max: {} ms, Avg: {} ms, P50: {} ms, P95: {} ms, P99: {} ms",
            min, max, String.format("%.2f", average), p50, p95, p99);

        return new LatencyMetrics(min, max, average, p50, p95, p99);
    }

    /**
     * Calculates the specified percentile from a sorted list of latencies.
     *
     * @param sortedLatencies Sorted list of latency values
     * @param percentile      Percentile to calculate (0-100)
     * @return                Latency value at the specified percentile
     */
    private static long calculatePercentile(final List<Long> sortedLatencies,
                                            final int percentile) {
        if (sortedLatencies.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.size()) - 1;
        index = Math.max(0, Math.min(index, sortedLatencies.size() - 1));
        return sortedLatencies.get(index);
    }

    /**
     * Extracts the user index from a user name.
     *
     * @param userName      the full user name (e.g., "my-user-5")
     * @param userPrefix    the prefix of the user name (e.g., "my-user")
     * @return              the extracted index, or -1 if not found
     */
    private static int extractUserIndex(String userName, String userPrefix) {
        if (userName.startsWith(userPrefix + "-")) {
            try {
                return Integer.parseInt(userName.substring(userPrefix.length() + 1));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Data class to hold latency metrics statistics.
     */
    public record LatencyMetrics(long min, long max, double average, long p50, long p95, long p99) { }
}
