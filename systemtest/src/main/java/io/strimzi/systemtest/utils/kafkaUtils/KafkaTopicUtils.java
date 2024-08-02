/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

public class KafkaTopicUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private static final String TOPIC_NAME_PREFIX = "my-topic-";
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();
    private static final Random RANDOM = new Random();

    private KafkaTopicUtils() {}

    /**
     * Generated random name for the KafkaTopic resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfTopic() {
        String salt = RANDOM.nextInt(Integer.MAX_VALUE) + "-" + RANDOM.nextInt(Integer.MAX_VALUE);

        return  TOPIC_NAME_PREFIX + salt;
    }

    /**
     * Method which return UID for specific topic
     * @param namespace Namespace name
     * @param topicName Topic name
     * @return topic UID
     */
    public static String topicSnapshot(final String namespace, String topicName) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getMetadata().getUid();
    }

    public static void waitUntilTopicObservationGenerationIsPresent(final String namespace, final String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} observation generation", namespace, topicName);
        waitForCondition("KafkaTopic: " + namespace + "/" + topicName + " observation generation",
            namespace, topicName,
                kafkaTopic -> kafkaTopic.getStatus().getObservedGeneration() >= 0.0,
                TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
    }

    public static long topicObservationGeneration(final String namespace, final String topicName) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus().getObservedGeneration();
    }

    public static long waitTopicHasRolled(final String namespace, final String topicName, final long oldTopicObservation) {
        TestUtils.waitFor("Topic: " + namespace + "/" + topicName + " has rolled", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                // new observation has to be always higher number
                () -> oldTopicObservation < topicObservationGeneration(namespace, topicName));
        return topicObservationGeneration(namespace, topicName);
    }

    /**
     * Method which wait until topic has rolled form one generation to another.
     * @param namespace name of the namespace
     * @param topicName topic name
     * @param topicUid topic UID
     * @return topic new UID
     */
    public static String waitTopicHasRolled(final String namespace, String topicName, String topicUid) {
        TestUtils.waitFor("Topic: " + namespace + "/" + topicName + " has rolled", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> !topicUid.equals(topicSnapshot(namespace, topicName)));
        return topicSnapshot(namespace, topicName);
    }

    public static void waitForKafkaTopicCreation(String namespace, String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} creation ", namespace, topicName);
        TestUtils.waitFor("creation of KafkaTopic: " + namespace + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespace)
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals(Ready.toString()),
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicCreationByNamePrefix(String namespace, String topicNamePrefix) {
        LOGGER.info("Waiting for Topic {}/{} creation", namespace, topicNamePrefix);
        TestUtils.waitFor("creation of KafkaTopic: " + namespace + "/" + topicNamePrefix, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems().stream()
                    .filter(topic -> topic.getMetadata().getName().contains(topicNamePrefix))
                    .findFirst().orElseThrow().getStatus().getConditions().get(0).getType().equals(Ready.toString())
        );
    }

    public static void waitForKafkaTopicDeletion(String namespace, String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} deletion", namespace, topicName);
        TestUtils.waitFor("deletion of KafkaTopic: " + namespace + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("KafkaTopic: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespace, topicName);
                    cmdKubeClient(namespace).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicPartitionChange(String namespace, String topicName, int partitions) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to change", namespace, topicName);
        TestUtils.waitFor("change of KafkaTopic: " + namespace + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getSpec().getPartitions() == partitions,
            () -> LOGGER.error("KafkaTopic: {}/{} did not change partition", namespace, KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicReplicasChange(String namespace, String topicName, int replicas) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to change", namespace, topicName);
        TestUtils.waitFor("change of KafkaTopic: " + namespace + "/" + topicName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getSpec().getReplicas() == replicas,
            () -> LOGGER.error("KafkaTopic: {}/{} did not change replicas", namespace, KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get())
        );
    }

    /**
     * Wait until KafkaTopic is in desired status
     * @param namespace Namespace name
     * @param topicName name of KafkaTopic
     * @param conditionType desired state
     */
    public static boolean waitForKafkaTopicStatus(String namespace, String topicName, Enum<?> conditionType) {
        return waitForKafkaTopicStatus(namespace, topicName, conditionType, ConditionStatus.True);
    }

    public static boolean waitForKafkaTopicStatus(String namespace, String topicName, Enum<?> conditionType, ConditionStatus conditionStatus) {
        return ResourceManager.waitForResourceStatus(namespace, KafkaTopicResource.kafkaTopicClient(), KafkaTopic.RESOURCE_KIND,
            topicName, conditionType, conditionStatus, ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND));
    }

    public static boolean waitForKafkaTopicReady(String namespace, String topicName) {
        return waitForKafkaTopicStatus(namespace, topicName, Ready);
    }

    public static boolean waitForKafkaTopicNotReady(final String namespace, String topicName) {
        return waitForKafkaTopicStatus(namespace, topicName, Ready, ConditionStatus.False);
    }

    public static void waitForTopicConfigContains(String namespace, String topicName, Map<String, Object> config) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to contain correct config", namespace, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespace + "/" + topicName + " to contain correct config",
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
                () -> KafkaTopicUtils.configsAreEqual(KafkaTopicResource.kafkaTopicClient()
                        .inNamespace(namespace).withName(topicName).get().getSpec().getConfig(), config)
        );
        LOGGER.info("KafkaTopic: {}/{} contains correct config", namespace, topicName);
    }

    public static boolean configsAreEqual(Map<String, Object> actualConf, Map<String, Object> expectedConf) {
        if ((actualConf != null && expectedConf != null) && (expectedConf.size() == actualConf.size())) {
            return expectedConf.entrySet().stream()
                    .allMatch(expected -> expected.getValue().toString().equals(actualConf.get(expected.getKey()).toString()));
        }
        return false;
    }

    public static void waitForKafkaTopicSpecStability(final String namespace, String topicName, String scraperPodName, String bootstrapServer) {
        int[] stableCounter = {0};

        String oldSpec = KafkaCmdClient.describeTopicUsingPodCli(namespace, scraperPodName, bootstrapServer, topicName);

        TestUtils.waitFor("KafkaTopic's spec to be stable", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (oldSpec.equals(KafkaCmdClient.describeTopicUsingPodCli(namespace, scraperPodName, bootstrapServer, topicName))) {
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
        return KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems()
            .stream().filter(p -> p.getMetadata().getName().startsWith(prefix))
            .collect(Collectors.toList());
    }

    public static void deleteAllKafkaTopicsByPrefixWithWait(String namespace, String prefix) {
        KafkaTopicUtils.getAllKafkaTopicsWithPrefix(namespace, prefix).forEach(topic ->
            cmdKubeClient().namespace(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topic.getMetadata().getName())
        );
    }

    public static void waitForTopicsByPrefixDeletionUsingPodCli(String namespace, String prefix, String bootstrapName, String scraperPodName, String properties) {
        LOGGER.info("Waiting for all Topics with prefix: {} to be deleted from Kafka", prefix);
        TestUtils.waitFor("deletion of all Topics with prefix: " + prefix, TestConstants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> !KafkaCmdClient.listTopicsUsingPodCliWithConfigProperties(namespace, scraperPodName, bootstrapName, properties).contains(prefix));
    }

    public static void waitForDeletionOfTopicsWithPrefix(String topicPrefix, AdminClient adminClient) {
        LOGGER.info("Waiting for all Topics with prefix: {} to be deleted from Kafka", topicPrefix);
        TestUtils.waitFor("deletion of all Topics with prefix: " + topicPrefix, TestConstants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> !adminClient.listTopics().contains(topicPrefix));
    }

    public static void waitForTopicWillBePresentInKafka(String namespace, String topicName, String bootstrapName, String scraperPodName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to be present in Kafka", namespace, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespace + "/" + topicName + " to be present in Kafka", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> KafkaCmdClient.listTopicsUsingPodCli(namespace, scraperPodName, bootstrapName).contains(topicName));
    }

    public static List<String> getKafkaTopicReplicasForEachPartition(String namespace, String topicName, String podName, String bootstrapServer) {
        return Arrays.stream(KafkaCmdClient.describeTopicUsingPodCli(namespace, podName, bootstrapServer, topicName)
            .replaceFirst("Topic.*\n", "")
            .replaceAll(".*Replicas: ", "")
            .replaceAll("\tIsr.*", "")
            .split("\n"))
            .collect(Collectors.toList());
    }

    public static void waitForTopicWithPrefixDeletion(String namespace, String topicPrefix) {
        TestUtils.waitFor("deletion of all topics with prefix: " + topicPrefix, TestConstants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> {
                try {
                    final int numberOfTopicsToDelete = getAllKafkaTopicsWithPrefix(namespace, topicPrefix).size();
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
     * @param namespace Namespace name
     * @param queryingPodName  the name of the pod to query KafkaTopic from
     * @param clusterName name of Kafka cluster
     * @param absentTopicName name of Kafka topic which should not be created
     * @param topicOperatorReconciliationMs interval for Topic Operator to reconcile
     * @throws AssertionError in case topic is created
     */
    public static void verifyUnchangedTopicAbsence(String namespace, String queryingPodName, String clusterName, String absentTopicName, long topicOperatorReconciliationMs) {
        long endTime = System.currentTimeMillis() + 2 * topicOperatorReconciliationMs;

        LOGGER.info("Verifying absence of Topic: {}/{} in listed KafkaTopic(s) for next {} second(s)", namespace, absentTopicName, topicOperatorReconciliationMs / 1000);

        while (System.currentTimeMillis() < endTime) {
            assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespace, queryingPodName, KafkaResources.plainBootstrapAddress(clusterName)), not(hasItems(absentTopicName)));
            try {
                Thread.sleep(TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void setFinalizersInAllTopicsToNull(String namespace) {
        LOGGER.info("Setting finalizers in all KafkaTopics in Namespace: {} to null", namespace);
        KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems().forEach(kafkaTopic ->
            KafkaTopicResource.replaceTopicResourceInSpecificNamespace(namespace, kafkaTopic.getMetadata().getName(), kt -> kt.getMetadata().setFinalizers(null))
        );
    }

    public static void waitForTopicStatusMessage(String namespace, String topicName, String message) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to contain message: {} in its status", namespace, topicName, message);

        TestUtils.waitFor(String.join("KafkaTopic: %s/%s status to contain message: %s", namespace, topicName, message), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get()
                        .getStatus().getConditions().stream().anyMatch(condition -> condition.getMessage().contains(message))
        );
    }

    /**
     * Waits for a specific condition to be met on a KafkaTopic within a namespace.
     *
     * @param waitDescription   A human-readable description of the condition being waited on.
     * @param namespace     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic to check.
     * @param condition         A Predicate that takes a KafkaTopic and returns true if the condition is met.
     */
    private static void waitForCondition(String waitDescription, String namespace, String topicName,
                                        Predicate<KafkaTopic> condition, long timeout) {
        LOGGER.info("Starting wait for condition: {}", waitDescription);
        TestUtils.waitFor(waitDescription,
                TestConstants.GLOBAL_POLL_INTERVAL, timeout,
                () -> {
                    KafkaTopic kafkaTopic = KafkaTopicResource.getKafkaTopic(namespace, topicName);
                    return condition.test(kafkaTopic);
                });
        LOGGER.info("Condition '{}' met for KafkaTopic: {}/{}", waitDescription, namespace, topicName);
    }

    /**
     * Waits for a replica change failure due to insufficient brokers to be reported in a KafkaTopic's status.
     *
     * @param namespace     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic.
     * @param targetReplicas    The target replica count that was attempted to be set.
     */
    public static void waitForReplicaChangeFailureDueToInsufficientBrokers(String namespace, String topicName, int targetReplicas) {
        waitForCondition("Replica change failure due to insufficient brokers",
            namespace, topicName,
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
     * @param namespace     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic to check.
     */
    public static void waitForReplicaChangeStatusNotPresent(String namespace, String topicName) {
        final int[] successCounter = new int[]{0};
        final int[] totalSuccessThreshold = new int[]{10};

        waitForCondition("replica change status not present",
            namespace, topicName,
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
     * @param namespace     The Kubernetes namespace in which the KafkaTopic resides.
     * @param topicName         The name of the KafkaTopic.
     */
    public static void waitUntilReplicaChangeOngoing(String namespace, String topicName) {
        waitForCondition("replicaChange of %s in 'ongoing' state".formatted(topicName),
            namespace, topicName,
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
     * @param namespace the Kubernetes namespace in which the KafkaTopic resides
     * @param topicName the name of the KafkaTopic to check for replica change resolution
     */
    public static void waitUntilReplicaChangeResolved(String namespace, String topicName) {
        final int[] successCounter = new int[]{0};
        final int[] totalSuccessThreshold = new int[]{10};

        waitForCondition("resolution of replica change",
            namespace, topicName,
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

        LOGGER.info("Replica change resolved for KafkaTopic: {}/{}", namespace, topicName);
    }
}
