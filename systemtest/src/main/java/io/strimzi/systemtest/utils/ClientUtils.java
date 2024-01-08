/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

/**
 * ClientUtils class, which provides static methods for the all type clients
 * @see io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient
 */
public class ClientUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClientUtils.class);
    private static final String CONSUMER_GROUP_NAME = "my-consumer-group-";
    private static Random rng = new Random();

    // ensuring that object can not be created outside of class
    private ClientUtils() {}

    // Both clients success
    public static void waitForClientsSuccess(TestStorage testStorage) {
        waitForClientsSuccess(testStorage, true);
    }

    public static void waitForClientsSuccess(TestStorage testStorage, boolean deleteAfterSuccess) {
        waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount(), deleteAfterSuccess);
    }

    public static void waitForClientsSuccess(String producerName, String consumerName, String namespace, int messageCount) {
        waitForClientsSuccess(producerName, consumerName, namespace, messageCount, true);
    }

    public static void waitForClientsSuccess(String producerName, String consumerName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer: {}/{} and consumer: {}/{} Jobs to finish successfully", namespace, producerName, namespace, consumerName);
        TestUtils.waitFor("client Jobs to finish successfully", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> kubeClient().checkSucceededJobStatus(namespace, producerName, 1)
                && kubeClient().checkSucceededJobStatus(namespace, consumerName, 1),
            () -> {
                JobUtils.logCurrentJobStatus(producerName, namespace);
                JobUtils.logCurrentJobStatus(consumerName, namespace);
            });

        if (deleteAfterSuccess) {
            JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
        }
    }

    // Client success
    public static void waitForConsumerClientSuccess(TestStorage testStorage) {
        waitForClientSuccess(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    public static void waitForProducerClientSuccess(TestStorage testStorage) {
        waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    public static void waitForClientSuccess(String jobName, String namespace, int messageCount) {
        waitForClientSuccess(jobName, namespace, messageCount, true);
    }

    public static void waitForClientSuccess(String jobName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for client Job: {}/{} to finish successfully", namespace, jobName);
        TestUtils.waitFor("client Job to finish successfully", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> {
                LOGGER.debug("Client Job: {}/{} has status {}", namespace, jobName, kubeClient().namespace(namespace).getJobStatus(jobName));
                return kubeClient().checkSucceededJobStatus(namespace, jobName, 1);
            },
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    // Client timeouts
    public static void waitForProducerClientTimeout(TestStorage testStorage) {
        waitForProducerClientTimeout(testStorage, true);
    }

    public static void waitForProducerClientTimeout(TestStorage testStorage, boolean deleteAfterSuccess) {
        waitForClientTimeout(testStorage.getProducerName(), testStorage.getNamespaceName(), testStorage.getMessageCount(), deleteAfterSuccess);
    }

    public static void waitForConsumerClientTimeout(TestStorage testStorage) {
        waitForConsumerClientTimeout(testStorage, true);
    }

    public static void waitForConsumerClientTimeout(TestStorage testStorage, boolean deleteAfterSuccess) {
        waitForClientTimeout(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount(), deleteAfterSuccess);
    }

    public static void waitForClientTimeout(String jobName, String namespace, int messageCount) {
        waitForClientTimeout(jobName, namespace, messageCount, true);
    }

    public static void waitForClientTimeout(String jobName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for client Job: {}/{} to reach the timeout limit", namespace, jobName);
        try {
            TestUtils.waitFor("client Job: " + namespace + "/" + jobName + "to reach the the timeout limit", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespace, jobName, 1),
                () -> JobUtils.logCurrentJobStatus(jobName, namespace));

            if (deleteAfterSuccess) {
                JobUtils.deleteJobWithWait(namespace, jobName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Client Job: {}/{} reached the expected timeout", namespace, jobName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobWithWait(namespace, jobName);
                }
            } else {
                JobUtils.logCurrentJobStatus(jobName, namespace);
                throw e;
            }
        }
    }

    // Both clients timeouts
    public static void waitForClientsTimeout(TestStorage testStorage) {
        waitForClientsTimeout(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    public static void waitForClientsTimeout(String producerName, String consumerName, String namespace, int messageCount) {
        waitForClientsTimeout(producerName, consumerName, namespace, messageCount, true);
    }

    public static void waitForClientsTimeout(String producerName, String consumerName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer {}/{} and consumer {}/{} Jobs to reach the timeout limit", namespace, producerName, namespace, consumerName);

        try {
            TestUtils.waitFor("client Jobs: " + producerName + " and " + consumerName + " in Namespace: " + namespace + " to reach the timeout limit", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespace, producerName, 1)
                    && kubeClient().checkFailedJobStatus(namespace, consumerName, 1),
                () -> {
                    JobUtils.logCurrentJobStatus(producerName, namespace);
                    JobUtils.logCurrentJobStatus(consumerName, namespace);
                });

            if (deleteAfterSuccess) {
                JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Client Jobs {}/{} and {}/{} reached the expected timeout", namespace, producerName, namespace, consumerName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
                }
            } else {
                throw e;
            }
        }
    }

    public static void waitForClientContainsAllMessages(String jobName, String namespace, List<String> messages, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespace, jobName);
        List<String> notReadyMessages = messages;
        TestUtils.waitFor("client Job to contain all messages: [" + messages.toString() + "]", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.THROTTLING_EXCEPTION_TIMEOUT, () -> {
            for (String message : messages) {
                if (kubeClient().logsInSpecificNamespace(namespace, jobPodName).contains(message)) {
                    notReadyMessages.remove(message);
                }
            }

            if (deleteAfterSuccess && notReadyMessages.isEmpty()) {
                JobUtils.deleteJobWithWait(namespace, jobName);
            }

            return notReadyMessages.isEmpty();
        });
    }

    public static void waitForClientContainsMessage(String jobName, String namespace, String message) {
        waitForClientContainsMessage(jobName, namespace, message, true);
    }

    public static void waitForClientContainsMessage(String jobName, String namespace, String message, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespace, jobName);
        LOGGER.info("Waiting for client Job: {}/{} to contain message: [{}]", namespace, jobName, message);

        TestUtils.waitFor("client Job to contain message: [" + message + "]", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.THROTTLING_EXCEPTION_TIMEOUT,
            () -> kubeClient().logsInSpecificNamespace(namespace, jobPodName).contains(message),
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    public static void waitForClientNotContainsMessage(String jobName, String namespace, String message) {
        waitForClientNotContainsMessage(jobName, namespace, message, true);
    }

    public static void waitForClientNotContainsMessage(String jobName, String namespace, String message, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespace, jobName);
        LOGGER.info("Waiting for client Job: {}/{} to not contain message: [{}]", namespace, jobName, message);

        TestUtils.waitFor("client Job to contain message: [" + message + "]", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.THROTTLING_EXCEPTION_TIMEOUT,
            () -> !kubeClient().logsInSpecificNamespace(namespace, jobPodName).contains(message),
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    private static long timeoutForClientFinishJob(int messagesCount) {
        // need to add at least 2minutes for finishing the job
        return (long) messagesCount * 1000 + Duration.ofMinutes(2).toMillis();
    }

    /**
     * Method which generates random consumer group name
     * @return consumer group name with pattern: my-consumer-group-*-*
     */
    public static String generateRandomConsumerGroup() {
        int salt = rng.nextInt(Integer.MAX_VALUE);

        return CONSUMER_GROUP_NAME + salt;
    }

    public static KafkaClientsBuilder getDefaultClientBuilder(TestStorage testStorage) {
        return new KafkaClientsBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .withTopicName(testStorage.getTopicName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName());
    }
}

