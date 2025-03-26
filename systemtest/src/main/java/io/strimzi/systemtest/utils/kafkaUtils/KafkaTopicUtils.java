/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.skodjob.testframe.executor.ExecResult;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.CrdClients.kafkaTopicClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class KafkaTopicUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private static final String TOPIC_NAME_PREFIX = "my-topic-";
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();
    private static final Random RANDOM = new Random();

    private KafkaTopicUtils() {}

    /**
     * Replaces KafkaTopic in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName     name of the Namespace where the resource should be replaced.
     * @param resourceName      name of the KafkaTopic's name.
     * @param editor            editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<KafkaTopic> editor) {
        KafkaTopic kafkaTopic = kafkaTopicClient().inNamespace(namespaceName).withName(resourceName).get();
        KubeResourceManager.get().replaceResourceWithRetries(kafkaTopic, editor);
    }

    /**
     * Generated random name for the KafkaTopic resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfTopic() {
        String salt = RANDOM.nextInt(Integer.MAX_VALUE) + "-" + RANDOM.nextInt(Integer.MAX_VALUE);

        return  TOPIC_NAME_PREFIX + salt;
    }

    public static void waitUntilTopicObservationGenerationIsPresent(final String namespaceName, final String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} observation generation", namespaceName, topicName);
        waitForCondition("KafkaTopic: " + namespaceName + "/" + topicName + " observation generation",
                namespaceName, topicName,
                kafkaTopic -> kafkaTopic.getStatus().getObservedGeneration() >= 0.0,
                TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
    }

    public static long topicObservationGeneration(final String namespaceName, final String topicName) {
        return kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getStatus().getObservedGeneration();
    }

    public static long waitTopicHasRolled(final String namespaceName, final String topicName, final long oldTopicObservation) {
        TestUtils.waitFor("Topic: " + namespaceName + "/" + topicName + " has rolled", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                // new observation has to be always higher number
                () -> oldTopicObservation < topicObservationGeneration(namespaceName, topicName));
        return topicObservationGeneration(namespaceName, topicName);
    }

    public static void waitForKafkaTopicCreation(String namespaceName, String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} creation ", namespaceName, topicName);
        TestUtils.waitFor("creation of KafkaTopic: " + namespaceName + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kafkaTopicClient().inNamespace(namespaceName)
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals(Ready.toString()),
            () -> LOGGER.info(kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicDeletion(String namespaceName, String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} deletion", namespaceName, topicName);
        TestUtils.waitFor("deletion of KafkaTopic: " + namespaceName + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("KafkaTopic: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespaceName, topicName);
                    KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                    return false;
                }
            },
            () -> LOGGER.info(kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicPartitionChange(String namespaceName, String topicName, int partitions) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to change", namespaceName, topicName);
        TestUtils.waitFor("change of KafkaTopic: " + namespaceName + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getSpec().getPartitions() == partitions,
            () -> LOGGER.error("KafkaTopic: {}/{} did not change partition", namespaceName, kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicReplicasChange(String namespaceName, String topicName, int replicas) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to change", namespaceName, topicName);
        TestUtils.waitFor("change of KafkaTopic: " + namespaceName + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getSpec().getReplicas() == replicas,
            () -> LOGGER.error("KafkaTopic: {}/{} did not change replicas", namespaceName, kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    /**
     * Wait until KafkaTopic is in desired status
     * @param namespaceName Namespace name
     * @param topicName name of KafkaTopic
     * @param conditionType desired state
     */
    public static void waitForKafkaTopicStatus(String namespaceName, String topicName, Enum<?> conditionType) {
        waitForKafkaTopicStatus(namespaceName, topicName, conditionType, ConditionStatus.True);
    }

    public static void waitForKafkaTopicStatus(String namespaceName, String topicName, Enum<?> conditionType, ConditionStatus conditionStatus) {
        KafkaTopic kafkaTopic = kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get();
        KubeResourceManager.get().waitResourceCondition(kafkaTopic, ResourceConditions.resourceHasDesiredState(conditionType, conditionStatus), ResourceOperation.getTimeoutForResourceReadiness(kafkaTopic.getKind()));
    }

    public static void waitForKafkaTopicReady(String namespaceName, String topicName) {
        waitForKafkaTopicStatus(namespaceName, topicName, Ready);
    }

    public static void waitForKafkaTopicNotReady(final String namespaceName, String topicName) {
        waitForKafkaTopicStatus(namespaceName, topicName, Ready, ConditionStatus.False);
    }

    public static void waitForTopicConfigContains(String namespaceName, String topicName, Map<String, Object> config) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to contain correct config", namespaceName, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespaceName + "/" + topicName + " to contain correct config",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
                () -> KafkaTopicUtils.configsAreEqual(kafkaTopicClient()
                        .inNamespace(namespaceName).withName(topicName).get().getSpec().getConfig(), config)
        );
        LOGGER.info("KafkaTopic: {}/{} contains correct config", namespaceName, topicName);
    }

    public static boolean configsAreEqual(Map<String, Object> actualConf, Map<String, Object> expectedConf) {
        if ((actualConf != null && expectedConf != null) && (expectedConf.size() == actualConf.size())) {
            return expectedConf.entrySet().stream()
                    .allMatch(expected -> expected.getValue().toString().equals(actualConf.get(expected.getKey()).toString()));
        }
        return false;
    }

    public static void waitForKafkaTopicSpecStability(final String namespaceName, String topicName, String scraperPodName, String bootstrapServer) {
        int[] stableCounter = {0};

        String oldSpec = KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, bootstrapServer, topicName);

        TestUtils.waitFor("KafkaTopic's spec to be stable", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (oldSpec.equals(KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, bootstrapServer, topicName))) {
                stableCounter[0]++;
                if (stableCounter[0] == TestConstants.GLOBAL_STABILITY_OFFSET_COUNT) {
                    LOGGER.info("KafkaTopic's spec is stable for: {} poll intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("KafkaTopic's spec is not stable. Going to set the counter to zero");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("KafkaTopic's spec gonna be stable in {} polls", TestConstants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
            return false;
        });
    }

    public static List<KafkaTopic> getAllKafkaTopicsWithPrefix(String namespace, String prefix) {
        return kafkaTopicClient().inNamespace(namespace).list().getItems()
            .stream().filter(p -> p.getMetadata().getName().startsWith(prefix))
            .collect(Collectors.toList());
    }

    public static void waitForDeletionOfTopicsWithPrefix(String topicPrefix, AdminClient adminClient) {
        LOGGER.info("Waiting for all Topics with prefix: {} to be deleted from Kafka", topicPrefix);
        TestUtils.waitFor("deletion of all Topics with prefix: " + topicPrefix, TestConstants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> !adminClient.listTopics().contains(topicPrefix));
    }

    public static void waitForTopicWillBePresentInKafka(String namespaceName, String topicName, String bootstrapName, String scraperPodName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to be present in Kafka", namespaceName, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespaceName + "/" + topicName + " to be present in Kafka", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, bootstrapName).contains(topicName));
    }

    public static List<String> getKafkaTopicReplicasForEachPartition(String namespaceName, String topicName, String podName, String bootstrapServer) {
        return Arrays.stream(KafkaCmdClient.describeTopicUsingPodCli(namespaceName, podName, bootstrapServer, topicName)
            .replaceFirst("Topic.*\n", "")
            .replaceAll(".*Replicas: ", "")
            .replaceAll("\tIsr.*", "")
            .split("\n"))
            .collect(Collectors.toList());
    }

    /**
     * Waits for the topic replicas to be moved to the specified brokers.
     *
     * @param namespaceName   The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName       The name of the KafkaTopic.
     * @param scraperPodName  The name of the pod used to scrape Kafka metrics.
     * @param bootstrapServer The bootstrap server address.
     * @param brokerIds       A list of broker IDs where replicas should be moved.
     */
    public static void waitForTopicReplicasOnBrokers(String namespaceName, String topicName, String scraperPodName, String bootstrapServer, List<String> brokerIds) {
        LOGGER.info("Waiting for topic replicas of {} to be moved to brokers {}", topicName, brokerIds);
        TestUtils.waitFor(
            "Wait for topic replicas to be on the specified brokers",
            TestConstants.GLOBAL_POLL_INTERVAL,
            TestConstants.GLOBAL_TIMEOUT,
            () -> {
                List<String> topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(
                    namespaceName,
                    topicName,
                    scraperPodName,
                    bootstrapServer
                );
                return topicReplicas.stream().anyMatch(line -> brokerIds.stream().anyMatch(line::contains));
            }
        );
    }

    public static void waitForTopicWithPrefixDeletion(String namespaceName, String topicPrefix) {
        TestUtils.waitFor("deletion of all topics with prefix: " + topicPrefix, TestConstants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> {
                try {
                    final int numberOfTopicsToDelete = getAllKafkaTopicsWithPrefix(namespaceName, topicPrefix).size();
                    LOGGER.info("Remaining KafkaTopic's to delete: {} !", numberOfTopicsToDelete);
                    return numberOfTopicsToDelete == 0;
                } catch (Exception e) {
                    return e.getMessage().contains("Not Found") || e.getMessage().contains("the server doesn't have a resource type");
                }
            });
    }

    /**
     * Verifies that {@code absentTopicName} topic remains absent in {@code clusterName} Kafka cluster residing in {@code namespaceName},
     * for two times {@code topicOperatorReconciliationMs} duration of Topic Operator reconciliation time,
     * by querying the cluster using kafka scripts from {@code queryingPodName} Pod.
     *
     * @param namespaceName Namespace name
     * @param queryingPodName  the name of the pod to query KafkaTopic from
     * @param clusterName name of Kafka cluster
     * @param absentTopicName name of Kafka topic which should not be created
     * @param topicOperatorReconciliationMs interval for Topic Operator to reconcile
     * @throws AssertionError in case topic is created
     */
    public static void verifyUnchangedTopicAbsence(String namespaceName, String queryingPodName, String clusterName, String absentTopicName, long topicOperatorReconciliationMs) {
        long endTime = System.currentTimeMillis() + 2 * topicOperatorReconciliationMs;

        LOGGER.info("Verifying absence of Topic: {}/{} in listed KafkaTopic(s) for next {} second(s)", namespaceName, absentTopicName, topicOperatorReconciliationMs / 1000, namespaceName);

        while (System.currentTimeMillis() < endTime) {
            assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespaceName, queryingPodName, KafkaResources.plainBootstrapAddress(clusterName)), not(hasItems(absentTopicName)));
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS));
        }
    }

    public static void setFinalizersInAllTopicsToNull(String namespaceName) {
        LOGGER.info("Setting finalizers in all KafkaTopics in Namespace: {} to null", namespaceName);
        kafkaTopicClient().inNamespace(namespaceName).list().getItems().forEach(kafkaTopic ->
            KafkaTopicUtils.replace(namespaceName, kafkaTopic.getMetadata().getName(), kt -> kt.getMetadata().setFinalizers(null))
        );
    }

    public static void waitForTopicStatusMessage(String namespaceName, String topicName, String message) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to contain message: {} in its status", namespaceName, topicName, message);

        TestUtils.waitFor(String.format("KafkaTopic: %s/%s status to contain message: %s", namespaceName, topicName, message), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get()
                        .getStatus().getConditions().stream().anyMatch(condition -> condition.getMessage().contains(message))
        );
    }

    /**
     * Waits for a specific condition to be met on a KafkaTopic within a namespace.
     *
     * @param waitDescription   A human-readable description of the condition being waited on.
     * @param namespaceName     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic to check.
     * @param condition         A Predicate that takes a KafkaTopic and returns true if the condition is met.
     */
    private static void waitForCondition(String waitDescription, String namespaceName, String topicName,
                                        Predicate<KafkaTopic> condition, long timeout) {
        LOGGER.info("Starting wait for condition: {}", waitDescription);
        TestUtils.waitFor(waitDescription,
                TestConstants.GLOBAL_POLL_INTERVAL, timeout,
                () -> {
                    KafkaTopic kafkaTopic = kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get();
                    return condition.test(kafkaTopic);
                });
        LOGGER.info("Condition '{}' met for KafkaTopic: {}/{}", waitDescription, namespaceName, topicName);
    }

    /**
     * Waits for a replica change failure due to insufficient brokers to be reported in a KafkaTopic's status.
     *
     * @param namespaceName     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic.
     * @param targetReplicas    The target replica count that was attempted to be set.
     */
    public static void waitForReplicaChangeFailureDueToInsufficientBrokers(String namespaceName, String topicName, int targetReplicas) {
        waitForCondition("Replica change failure due to insufficient brokers",
                namespaceName, topicName,
                kafkaTopic -> checkReplicaChangeFailureDueToInsufficientBrokers(kafkaTopic, targetReplicas),
                TestConstants.CRUISE_CONTROL_TRAIN_MODEL_TIMEOUT);
    }

    /**
     * Checks if a KafkaTopic's replica change failed due to insufficient brokers.
     *
     * @param kafkaTopic        The KafkaTopic to check.
     * @param targetReplicas    The target replica count that was attempted to be set.
     * @return true             if the replica change failed due to insufficient brokers, otherwise false.
     */
    private static boolean checkReplicaChangeFailureDueToInsufficientBrokers(KafkaTopic kafkaTopic, int targetReplicas) {
        if (kafkaTopic != null && kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getReplicasChange() != null) {
            String message = kafkaTopic.getStatus().getReplicasChange().getMessage();
            return message != null &&
                    message.contains("Requested RF cannot be more than number of alive brokers") &&
                    kafkaTopic.getStatus().getReplicasChange().getState().toValue().equals("pending") &&
                    kafkaTopic.getStatus().getReplicasChange().getTargetReplicas() == targetReplicas;
        }
        return false;
    }

    public static boolean hasTopicInKafka(final String topicName, final String clusterName, final String scraperPodName) {
        LOGGER.info("Checking Topic: {} in Kafka", topicName);
        return KafkaCmdClient.listTopicsUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)).contains(topicName);
    }

    public static boolean hasTopicInCRK8s(final KafkaTopic kafkaTopic, final String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that Topic: {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    /**
     * Waits until the status of a KafkaTopic's replica change no longer presents.
     *
     * @param namespaceName     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic to check.
     */
    public static void waitForReplicaChangeStatusNotPresent(String namespaceName, String topicName) {
        final int[] successCounter = new int[]{0};
        final int[] totalSuccessThreshold = new int[]{10};

        waitForCondition("replica change status not present",
                namespaceName, topicName,
                kafkaTopic -> {
                    LOGGER.debug("Stability: {}/{} ", successCounter[0], totalSuccessThreshold[0]);

                    if (kafkaTopic != null && kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getReplicasChange() == null) {
                        successCounter[0]++;
                        return successCounter[0] == totalSuccessThreshold[0];
                    }
                    successCounter[0] = 0; // reset counter if condition not met
                    return false;
                }, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
    }

    /**
     * Waits for a KafkaTopic's replica change status to be marked as ongoing.
     *
     * @param namespaceName     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic.
     */
    public static void waitUntilReplicaChangeOngoing(String namespaceName, String topicName) {
        waitForCondition("replicaChange of %s in 'ongoing' state".formatted(topicName),
            namespaceName, topicName,
            kafkaTopic -> {
                if (kafkaTopic != null && kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getReplicasChange() != null) {
                    String replicasChangeState = kafkaTopic.getStatus().getReplicasChange().getState().toValue();
                    String expectedState = "ongoing";

                    LOGGER.debug("ReplicaChange state of KafkaTopic {} is {}. Expected value is {}", topicName, replicasChangeState, expectedState);

                    return Objects.equals(replicasChangeState, expectedState);
                }
                LOGGER.debug("KafkaTopic {} replicasChange status is missing, retrying.");

                return false;
            }, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
    }

    /**
     * Waits until the replica change for a KafkaTopic in a specified namespace and topic name is resolved.
     * By resolved, we mean that the specific {@code topicName} would not contain anything in the replicaChange status section.
     * Moreover, it would not contain a failure related to the Cruise Control model not being trained yet (e.g., Replicas change failed (500)),
     * or there will be no message at all (we also treat this as success) because we mark such scenarios as replicaChange
     * is not in the status. In other words no replicaChange status is present and everything related to replicaChange is re-solved
     * (i.e., not ongoing process).
     *
     * @param namespaceName the Kubernetes namespace in which the KafkaTopic resides
     * @param topicName the name of the KafkaTopic to check for replica change resolution
     */
    public static void waitUntilReplicaChangeResolved(String namespaceName, String topicName) {
        final int[] successCounter = new int[]{0};
        final int[] totalSuccessThreshold = new int[]{10};

        waitForCondition("resolution of replica change",
                namespaceName, topicName,
                kafkaTopic -> {
                    LOGGER.debug("Stability: {}/{} ", successCounter[0], totalSuccessThreshold[0]);

                    // Evaluates if the replica change condition has been resolved
                    if (kafkaTopic != null && kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getReplicasChange() != null) {
                        String message = kafkaTopic.getStatus().getReplicasChange().getMessage();

                        if (message == null || !message.contains("Replicas change failed (500)")) {
                            successCounter[0]++;

                            return successCounter[0] == totalSuccessThreshold[0];
                        } else {
                            successCounter[0] = 0;
                        }
                    }
                    successCounter[0] = 0;
                    return false;
                }, TestConstants.CRUISE_CONTROL_TRAIN_MODEL_TIMEOUT);

        LOGGER.info("Replica change resolved for KafkaTopic: {}/{}", namespaceName, topicName);
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
        List<KafkaTopic> topicsToDelete = getAllKafkaTopicsWithPrefix(namespace, prefix, start, end);

        topicsToDelete.forEach(topic ->
            KubeResourceManager.get().kubeCmdClient().inNamespace(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topic.getMetadata().getName())
        );

        LOGGER.info("Deleted Kafka topics from {} to {} in namespace {} with prefix {}", start, end, namespace, prefix);
    }

    public static List<KafkaTopic> getAllKafkaTopicsWithPrefix(String namespace, String prefix, int start, int end) {
        return kafkaTopicClient().inNamespace(namespace).list().getItems()
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
     * Retrieves partition directories for specified brokers and volumes.
     *
     * @param testStorage           contains cluster and topic information.
     * @param brokerAndVolumeIdsList list of brokers and their volumes to query.
     * @return a map of broker pod and volume paths to sets of partition IDs.
     * @throws RuntimeException if the command to list directories fails.
     */
    public static Map<String, Set<Integer>> getPartitionDirectories(final TestStorage testStorage,
                                                                    final List<BrokerAndVolumeIds> brokerAndVolumeIdsList) {
        final Map<String, Set<Integer>> partitionDirs = new HashMap<>();
        for (final BrokerAndVolumeIds brokerAndVolumeIds : brokerAndVolumeIdsList) {
            final String brokerPodName = KafkaComponents.getBrokerPodSetName(testStorage.getClusterName())
                + "-" + brokerAndVolumeIds.getBrokerId();
            for (final int volumeId : brokerAndVolumeIds.getVolumeIds()) {
                final String dataDirPath = KafkaUtils.getDataDirectoryPath(volumeId, brokerAndVolumeIds.getBrokerId());
                final String command = "ls " + dataDirPath;
                final ExecResult execResult = KafkaUtils.executeInBrokerPod(testStorage.getNamespaceName(), brokerPodName, volumeId, brokerAndVolumeIds.getBrokerId(), command);

                final String[] dirs = execResult.out().trim().split("\\s+");
                final Set<Integer> partitions = Arrays.stream(dirs)
                    .filter(dir -> dir.startsWith(testStorage.getTopicName()))
                    .map(dir -> {
                        Pattern pattern = Pattern.compile(Pattern.quote(testStorage.getTopicName()) + "-(\\d+)$");
                        Matcher matcher = pattern.matcher(dir);
                        if (matcher.matches()) {
                            return Integer.parseInt(matcher.group(1));
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

                final String key = brokerPodName + ":" + dataDirPath;
                partitionDirs.put(key, partitions);
            }
        }
        return partitionDirs;
    }

    /**
     * Verifies that partitions have been moved off specified disks after a rebalance.
     *
     * @param initialPartitionDirs  the initial mapping of data directories to partitions.
     * @param finalPartitionDirs    the final mapping of data directories to partitions.
     * @throws AssertionError       if any initial partitions remain in their original directories.
     */
    public static void verifyPartitionsMovedOffDisks(final Map<String, Set<Integer>> initialPartitionDirs,
                                                     final Map<String, Set<Integer>> finalPartitionDirs) {
        for (final Map.Entry<String, Set<Integer>> entry : initialPartitionDirs.entrySet()) {
            final String key = entry.getKey();
            final Set<Integer> initialPartitions = initialPartitionDirs.get(key);
            final Set<Integer> finalPartitions = finalPartitionDirs.getOrDefault(key, Collections.emptySet());

            LOGGER.info("Data directory {}: initial partitions = {}, final partitions = {}", key, initialPartitions, finalPartitions);

            final Set<Integer> remainingPartitions = new HashSet<>(initialPartitions);
            remainingPartitions.retainAll(finalPartitions);

            assertThat("Partitions " + remainingPartitions + " are still present in data directory " + key + " after rebalance.",
                remainingPartitions.isEmpty(), is(true));
        }
    }

    /**
     * Gets the sizes of Kafka data directories for specified brokers and volumes.
     *
     * @param testStorage           cluster and namespace information.
     * @param brokerAndVolumeIdsList list of brokers and their volumes to query.
     * @return a map of broker pod and volume paths to directory sizes in bytes.
     * @throws RuntimeException if directory size retrieval fails.
     */
    public static Map<String, Long> getDataDirectorySizes(final TestStorage testStorage,
                                                          final List<BrokerAndVolumeIds> brokerAndVolumeIdsList) {
        final Map<String, Long> dataDirSizes = new HashMap<>();
        for (final BrokerAndVolumeIds brokerAndVolumeIds : brokerAndVolumeIdsList) {
            final String brokerPodName = KafkaComponents.getBrokerPodSetName(testStorage.getClusterName())
                + "-" + brokerAndVolumeIds.getBrokerId();
            for (final int volumeId : brokerAndVolumeIds.getVolumeIds()) {
                final String dataDirPath = KafkaUtils.getDataDirectoryPath(volumeId, brokerAndVolumeIds.getBrokerId());
                final String command = "du -sb " + dataDirPath + " | cut -f1";
                final ExecResult execResult = KafkaUtils.executeInBrokerPod(testStorage.getNamespaceName(), brokerPodName, volumeId, brokerAndVolumeIds.getBrokerId(), command);

                final long sizeInBytes = Long.parseLong(execResult.out().trim());
                final String key = brokerPodName + ":" + dataDirPath;
                dataDirSizes.put(key, sizeInBytes);
            }
        }
        return dataDirSizes;
    }

    /**
     * Verifies that data has been moved off the specified disks after a rebalance.
     *
     * @param initialDataDirSizes   a map of initial data directory paths to their sizes in bytes.
     * @param finalDataDirSizes     a map of final data directory paths to their sizes in bytes.
     * @throws AssertionError       if the size of any data directory has not been sufficiently reduced.
     */
    public static void verifyDataMovedOffSpecifiedDisks(final Map<String, Long> initialDataDirSizes,
                                                        final Map<String, Long> finalDataDirSizes) {
        LOGGER.info("Verifying that data has been moved off the specified disks...");

        for (final Map.Entry<String, Long> entry : initialDataDirSizes.entrySet()) {
            final String key = entry.getKey();
            final long initialSize = initialDataDirSizes.get(key);
            final long finalSize = finalDataDirSizes.getOrDefault(key, 0L);

            LOGGER.info("Data directory {}: initial size = {}, final size = {}", key, initialSize, finalSize);

            // Assert that final size is significantly less than initial size

            // Data directories
            //  --- before rebalance
            //
            //  cluster-ef377b8e-b-45e0111e-0:/var/lib/kafka/data-1 -> 524,437,387 bytes (~524 MB)
            //  cluster-ef377b8e-b-45e0111e-0:/var/lib/kafka/data-2 -> 524,450,596 bytes (~524 MB)
            //  cluster-ef377b8e-b-45e0111e-2:/var/lib/kafka/data-1 -> 524,436,977 bytes (~524 MB)
            // ------
            //  --- after rebalance
            //  cluster-ef377b8e-b-45e0111e-0:/var/lib/kafka/data-1 -> 24,982 bytes (~25 KB)
            //  cluster-ef377b8e-b-45e0111e-0:/var/lib/kafka/data-2 -> 37,416 bytes (~37 KB)
            //  cluster-ef377b8e-b-45e0111e-2:/var/lib/kafka/data-1 -> 24,983 bytes (~25 KB)
            assertThat("Expected data directory " + key + " to have reduced size after rebalance.",
                finalSize, Matchers.lessThan(initialSize / 100));
        }
    }
}
