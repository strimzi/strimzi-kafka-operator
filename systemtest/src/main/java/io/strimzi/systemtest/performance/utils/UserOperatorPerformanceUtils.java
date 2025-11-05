/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Utility class for managing KafkaUser resources in performance testing scenarios. It provides methods to create,
 * alter, and monitor KafkaUsers efficiently, supporting performance evaluations of the User Operator.
 */
public class UserOperatorPerformanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorPerformanceUtils.class);

    private static ExecutorService executorService = getCustomThreadPool();

    // ensuring that object can not be created outside of class
    private UserOperatorPerformanceUtils() {}

    private static ExecutorService getCustomThreadPool() {
        return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10);
    }

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

        listOfUsers.replaceAll(kafkaUser -> new KafkaUserBuilder(kafkaUser)
            .editSpec()
                .withAuthorization(updatedAcl)
                .withQuotas(kafkaUserQuotas)
            .endSpec()
            .build());

        KubeResourceManager.get().updateResource(listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));

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

    /**
     * Manages the full lifecycle of Kafka users concurrently using a cached thread pool.
     * This method processes creation, modification, and deletion for each user in separate threads,
     * allowing unlimited concurrent operations which is optimal for I/O-bound Kubernetes API calls.
     *
     * @param testStorage           An instance of TestStorage containing configuration and state needed for user operations.
     * @param numberOfUsers         The number of Kafka users to be processed.
     * @param spareEvents           The number of spare events to be consumed during the process.
     * @param warmUpTasksToProcess  The number of tasks to warm-up performance and optimize JIT. This number is used just for offsetting.
     *
     * @return                      The total time taken to complete all user lifecycles in milliseconds.
     */
    public static long processAllUsersConcurrently(TestStorage testStorage, int numberOfUsers, int spareEvents, int warmUpTasksToProcess) {
        if (executorService.isShutdown() || executorService.isTerminated()) {
            executorService = getCustomThreadPool();
            LOGGER.info("Reinitialized ExecutorService for new test run.");
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ExtensionContext extensionContext = KubeResourceManager.get().getTestContext();

        long startTime = System.nanoTime();

        for (int userIndex = warmUpTasksToProcess; userIndex < numberOfUsers + warmUpTasksToProcess; userIndex++) {
            final int startIndex = userIndex;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> performFullLifecycle(startIndex,  testStorage, extensionContext), executorService);
            futures.add(future);
        }

        // consume spare events
        for (int j = 0; j < spareEvents; j++) {
            futures.add(j, CompletableFuture.completedFuture(null));
        }

        // Wait for all users to complete their lifecycle
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.info("All KafkaUser lifecycles completed.");

        long allTasksTimeMs = Duration.ofNanos(System.nanoTime() - startTime).toMillis();

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

}
