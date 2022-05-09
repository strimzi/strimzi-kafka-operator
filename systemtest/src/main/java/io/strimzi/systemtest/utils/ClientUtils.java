/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.KafkaClientOperations;
import io.strimzi.systemtest.kafkaclients.clients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.rmi.UnexpectedException;
import java.time.Duration;
import java.util.Random;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

/**
 * ClientUtils class, which provides static methods for the all type clients
 * @see io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.clients.InternalKafkaClient
 */
public class ClientUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClientUtils.class);
    private static final String CONSUMER_GROUP_NAME = "my-consumer-group-";
    private static Random rng = new Random();

    // ensuring that object can not be created outside of class
    private ClientUtils() {}

    public static void waitUntilClientReceivedMessagesTls(KafkaClientOperations kafkaClient, int exceptedMessages) throws Exception {
        for (int tries = 1; ; tries++) {
            int receivedMessages = kafkaClient.receiveMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);

            if (receivedMessages == exceptedMessages) {
                LOGGER.info("Consumer successfully consumed {} messages for the {} time", exceptedMessages, tries);
                break;
            }
            LOGGER.warn("Client not received excepted messages {}, instead received only {}!", exceptedMessages, receivedMessages);

            if (tries == 3) {
                throw new RuntimeException(String.format("Consumer wasn't able to consume %s messages for 3 times", exceptedMessages));
            }
        }
    }

    public static void waitForClientsSuccess(String producerName, String consumerName, String namespace, int messageCount) {
        waitForClientsSuccess(producerName, consumerName, namespace, messageCount, true);
    }

    public static void waitForClientsSuccess(String producerName, String consumerName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting till producer {} and consumer {} finish", producerName, consumerName);
        TestUtils.waitFor("clients finished", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
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

    public static void waitForClientSuccess(String jobName, String namespace, int messageCount) {
        waitForClientSuccess(jobName, namespace, messageCount, true);
    }

    public static void waitForClientSuccess(String jobName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer/consumer:{} to finished", jobName);
        TestUtils.waitFor("job finished", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> {
                LOGGER.debug("Job {} in namespace {}, has status {}", jobName, namespace, kubeClient().namespace(namespace).getJobStatus(jobName));
                return kubeClient().checkSucceededJobStatus(namespace, jobName, 1);
            },
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    public static void waitForClientTimeout(String jobName, String namespace, int messageCount) throws UnexpectedException {
        waitForClientTimeout(jobName, namespace, messageCount, true);
    }

    public static void waitForClientTimeout(String jobName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer/consumer:{} to finish with failure.", jobName);
        try {
            TestUtils.waitFor("Job did not finish within time limit (as expected).", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespace, jobName, 1),
                () -> JobUtils.logCurrentJobStatus(jobName, namespace));

            if (deleteAfterSuccess) {
                JobUtils.deleteJobWithWait(namespace, jobName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Client job '{}' finished with expected timeout.", jobName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobWithWait(namespace, jobName);
                }
            } else {
                JobUtils.logCurrentJobStatus(jobName, namespace);
                throw e;
            }
        }
    }

    public static void waitForClientsTimeout(String producerName, String consumerName, String namespace, int messageCount) {
        waitForClientsTimeout(producerName, consumerName, namespace, messageCount, true);
    }

    public static void waitForClientsTimeout(String producerName, String consumerName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer: {} and consumer: {} to finish with failure.", producerName, consumerName);

        try {
            TestUtils.waitFor("clients timeout", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
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
                LOGGER.info("Both jobs - {} and {} - hit the wait timeout - as expected", producerName, consumerName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
                }
            } else {
                throw e;
            }
        }
    }

    public static void waitForClientContainsMessage(String jobName, String namespace, String message) {
        waitForClientContainsMessage(jobName, namespace, message, true);
    }

    public static void waitForClientContainsMessage(String jobName, String namespace, String message, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespace, jobName);
        LOGGER.info("Waiting for client:{} will contain message: {}", jobName, message);

        TestUtils.waitFor("job contains message " + message, Constants.GLOBAL_POLL_INTERVAL, Constants.THROTTLING_EXCEPTION_TIMEOUT,
            () -> kubeClient().logsInSpecificNamespace(namespace, jobPodName).contains(message),
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    private static long timeoutForClientFinishJob(int messagesCount) {
        // need to add at least 2minutes for finishing the job
        return (long) messagesCount * 1000 + Duration.ofMinutes(2).toMillis();
    }

    public static void waitUntilProducerAndConsumerSuccessfullySendAndReceiveMessages(ExtensionContext extensionContext,
                                                                                     InternalKafkaClient internalKafkaClient) throws Exception {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        ResourceManager.getInstance().createResource(extensionContext, KafkaTopicTemplates.topic(internalKafkaClient.getClusterName(), topicName)
            .editMetadata()
                .withNamespace(internalKafkaClient.getNamespaceName())
            .endMetadata()
            .build());

        InternalKafkaClient client = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .withNamespaceName(internalKafkaClient.getNamespaceName())
            .withTopicName(topicName)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            internalKafkaClient.getTopicName(), internalKafkaClient.getClusterName(), internalKafkaClient.getMessageCount());

        int sent = client.sendMessagesTls();
        int received = client.receiveMessagesTls();

        LOGGER.info("Sent {} and received {}", sent, received);

        if (sent != received) {
            throw new Exception("Client sent " + sent + " and received " + received + " ,which does not match!");
        }
    }

    /**
     * Method which generates random consumer group name
     * @return consumer group name with pattern: my-consumer-group-*-*
     */
    public static String generateRandomConsumerGroup() {
        int salt = rng.nextInt(Integer.MAX_VALUE);

        return CONSUMER_GROUP_NAME + salt;
    }
}

