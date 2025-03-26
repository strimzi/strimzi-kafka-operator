/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.PasswordBuilder;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthenticationBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserSpec;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.CrdClients.kafkaUserClient;

public class KafkaUserUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUserUtils.class);
    private static final String KAFKA_USER_NAME_PREFIX = "my-user-";
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();
    private static final Random RANDOM = new Random();

    private KafkaUserUtils() {}

    /**
     * Replaces KafkaUser in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName     name of the Namespace where the resource should be replaced.
     * @param resourceName      name of the KafkaUser's name.
     * @param editor            editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<KafkaUser> editor) {
        KafkaUser kafkaUser = kafkaUserClient().inNamespace(namespaceName).withName(resourceName).get();
        KubeResourceManager.get().replaceResourceWithRetries(kafkaUser, editor);
    }

    /**
     * Generated random name for the KafkaUser resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfKafkaUser() {
        String salt = RANDOM.nextInt(Integer.MAX_VALUE) + "-" + RANDOM.nextInt(Integer.MAX_VALUE);

        return  KAFKA_USER_NAME_PREFIX + salt;
    }

    public static void waitForKafkaUserCreation(String namespaceName, String userName) {
        KafkaUser kafkaUser = kafkaUserClient().inNamespace(namespaceName).withName(userName).get();

        SecretUtils.waitForSecretReady(namespaceName, userName,
            () -> LOGGER.info(kafkaUserClient().inNamespace(namespaceName).withName(userName).get()));

        KubeResourceManager.get().waitResourceCondition(kafkaUser, ResourceConditions.resourceIsReady(), ResourceOperation.getTimeoutForResourceReadiness(kafkaUser.getKind()));
    }

    public static void waitForKafkaUserDeletion(final String namespaceName, String userName) {
        LOGGER.info("Waiting for KafkaUser: {}/{} deletion", namespaceName, userName);
        TestUtils.waitFor("deletion of KafkaUser: " + namespaceName + "/" + userName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (kafkaUserClient().inNamespace(namespaceName).withName(userName).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("KafkaUser: {}/{} is not deleted yet! Triggering force delete via cmd client!", namespaceName, userName);
                    KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteByName(KafkaUser.RESOURCE_KIND, userName);
                    return false;
                }
            },
            () -> LOGGER.info(kafkaUserClient().inNamespace(namespaceName).withName(userName).get())
        );
        LOGGER.info("KafkaUser: {}/{} deleted", namespaceName, userName);
    }

    public static void waitForKafkaUserIncreaseObserverGeneration(String namespaceName, long observation, String userName) {
        TestUtils.waitFor("increase observation generation from " + observation + " for user " + userName,
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> observation < kafkaUserClient()
                .inNamespace(namespaceName).withName(userName).get().getStatus().getObservedGeneration());
    }

    public static void waitUntilKafkaUserStatusConditionIsPresent(String namespaceName, String userName) {
        LOGGER.info("Waiting for KafkaUser: {}/{} status to be available", namespaceName, userName);
        TestUtils.waitFor("KafkaUser " + userName + " status to be available", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> kafkaUserClient().inNamespace(namespaceName).withName(userName).get().getStatus().getConditions() != null,
            () -> LOGGER.info(kafkaUserClient().inNamespace(namespaceName).withName(userName).get())
        );
        LOGGER.info("KafkaUser: {}/{} status is available", namespaceName, userName);
    }

    /**
     * Wait until KafkaUser is in desired state
     * @param namespaceName Namespace name
     * @param userName name of KafkaUser
     * @param state desired state
     */
    public static void waitForKafkaUserStatus(String namespaceName, String userName, Enum<?> state) {
        KafkaUser kafkaUser = kafkaUserClient().inNamespace(namespaceName).withName(userName).get();
        KubeResourceManager.get().waitResourceCondition(kafkaUser, ResourceConditions.resourceHasDesiredState(state), ResourceOperation.getTimeoutForResourceReadiness(kafkaUser.getKind()));
    }

    public static void waitForKafkaUserNotReady(String namespaceName, String userName) {
        waitForKafkaUserStatus(namespaceName, userName, NotReady);
    }

    public static void waitForKafkaUserReady(String namespaceName, String userName) {
        waitForKafkaUserStatus(namespaceName, userName, Ready);
    }

    public static String removeKafkaUserPart(File kafkaUserFile, String partName) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(kafkaUserFile);
            ObjectNode kafkaUserSpec = (ObjectNode) node.at("/spec");
            kafkaUserSpec.remove(partName);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void waitForAllUsersWithPrefixReady(String namespaceName, String usersPrefix) {
        LOGGER.info("Waiting for all users with prefix: {} to become ready", usersPrefix);

        TestUtils.waitFor("all users to become ready", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT, () -> {
            List<KafkaUser> listOfUsers = kafkaUserClient().inNamespace(namespaceName).list().getItems().stream().filter(kafkaUser -> kafkaUser.getMetadata().getName().startsWith(usersPrefix)).toList();
            try {
                listOfUsers = listOfUsers.stream().filter(kafkaUser -> !(kafkaUser.getStatus().getConditions().stream().anyMatch(condition -> condition.getType().equals(Ready.toString()) && condition.getStatus().equals("True")))).toList();
                if (listOfUsers.size() != 0) {
                    LOGGER.warn("There are still {} users with prefix: {}, which are not in {} state", listOfUsers.size(), usersPrefix, Ready.toString());
                    return false;
                }
            } catch (RuntimeException e) {
                LOGGER.warn("There are still users with prefix: {}, which are not in {} state", usersPrefix, Ready.toString());
                return false;
            }
            LOGGER.info("All KafkaUsers with prefix: {} are ready", usersPrefix);
            return true;
        }, () -> LOGGER.error("Failed to wait for readiness state of these users: {}",
                kafkaUserClient().inNamespace(namespaceName).list().getItems().stream().filter(kafkaUser -> kafkaUser.getMetadata().getName().startsWith(usersPrefix)).toList()));
    }

    /**
     * Method which waits for all KafkaUser with specific prefix will contain desired KafkaUserSpec inside the
     * KafkaUser CR in specified namespace.
     *
     * @param namespaceName name of namespace, where KafkaUsers should be checked
     * @param usersPrefix prefix of KafkaUsers for which KafkaUserSpec will be checked
     * @param desiredUserSpec desired KafkaUserSpec for which we are waiting for
     */
    public static void waitForConfigToBeChangedInAllUsersWithPrefix(String namespaceName, String usersPrefix, KafkaUserSpec desiredUserSpec) {
        LOGGER.info("Waiting for all users with prefix: {} to contain desired config", usersPrefix);

        TestUtils.waitFor("all users to become ready", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT, () -> {
            List<KafkaUser> listOfUsers = kafkaUserClient().inNamespace(namespaceName).list().getItems().stream().filter(kafkaUser -> kafkaUser.getMetadata().getName().startsWith(usersPrefix)).toList();

            listOfUsers = listOfUsers.stream().filter(kafkaUser -> !kafkaUser.getSpec().equals(desiredUserSpec)).toList();

            if (listOfUsers.size() != 0) {
                LOGGER.warn("There are still {} users with prefix {}, which are not containing desired config", listOfUsers.size(), usersPrefix);
                return false;
            }

            LOGGER.info("All KafkaUsers with prefix: {} are containing desired config", usersPrefix);
            return true;
        }, () -> LOGGER.error("Failed to wait for readiness state of these users: {}",
                kafkaUserClient().inNamespace(namespaceName).list().getItems().stream().filter(kafkaUser -> kafkaUser.getMetadata().getName().startsWith(usersPrefix)).toList()));
    }

    public static void modifyKafkaUserPasswordWithNewSecret(String ns, String kafkaUserResourceName, String customSecretSource, String customPassword) {

        Secret userDefinedSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(customSecretSource)
                .withNamespace(ns)
            .endMetadata()
            .addToData("password", customPassword)
            .build();

        KubeResourceManager.get().createResourceWithWait(userDefinedSecret);

        KafkaUserUtils.replace(ns, kafkaUserResourceName, ku -> {

            ku.getSpec().setAuthentication(
                new KafkaUserScramSha512ClientAuthenticationBuilder()
                    .withPassword(
                        new PasswordBuilder()
                            .editOrNewValueFrom()
                                .withNewSecretKeyRef("password", customSecretSource, false)
                            .endValueFrom()
                            .build()
                    )
                    .build()
            );
        });

        waitForKafkaUserReady(ns, kafkaUserResourceName);
    }

    /**
     * Gets all KafkaUser resources in a specific namespace that start with a given prefix.
     *
     * @param namespace The Kubernetes namespace where the KafkaUser resources are located.
     * @param prefix The prefix to filter KafkaUser resources by their names.
     * @return A list of KafkaUser resources that start with the specified prefix.
     */
    public static List<KafkaUser> getAllKafkaUsersWithPrefix(String namespace, String prefix) {
        return kafkaUserClient().inNamespace(namespace).list().getItems()
            .stream().filter(p -> p.getMetadata().getName().startsWith(prefix))
            .collect(Collectors.toList());
    }

    /**
     * Waits for the deletion of all KafkaUser resources with a specific prefix in a namespace.
     *
     * @param namespaceName The namespace where the KafkaUser resources are located.
     * @param userPrefix The prefix of the KafkaUser resources to be deleted.
     */
    public static void waitForUserWithPrefixDeletion(String namespaceName, String userPrefix) {
        TestUtils.waitFor("deletion of all users with prefix: " + userPrefix, TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    return getAllKafkaUsersWithPrefix(namespaceName, userPrefix).size() == 0;
                } catch (Exception e) {
                    return e.getMessage().contains("Not Found") || e.getMessage().contains("the server doesn't have a resource type");
                }
            });
    }
}
