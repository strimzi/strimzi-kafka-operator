/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Random;

/**
 * ClientUtils class, which provides static methods for the all type clients
 * @see io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient
 */
public class ClientUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClientUtils.class);
    private static final String CONSUMER_GROUP_NAME = "my-consumer-group-";
    private static final Random RNG = new Random();

    // ensuring that object can not be created outside of class
    private ClientUtils() {}

    public static void waitForClientsSuccess(String namespaceName, String consumerName, String producerName, int messageCount) {
        waitForClientsSuccess(namespaceName, consumerName, producerName, messageCount, true);
    }

    public static void waitForClientsSuccess(String namespaceName, String consumerName, String producerName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer: {}/{} and consumer: {}/{} Jobs to finish successfully", namespaceName, producerName, namespaceName, consumerName);
        TestUtils.waitFor("client Jobs to finish successfully", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> JobUtils.checkSucceededJobStatus(namespaceName, producerName, 1)
                && JobUtils.checkSucceededJobStatus(namespaceName, consumerName, 1),
            () -> {
                JobUtils.logCurrentJobStatus(namespaceName, producerName);
                JobUtils.logCurrentJobStatus(namespaceName, consumerName);
            });

        if (deleteAfterSuccess) {
            JobUtils.deleteJobsWithWait(namespaceName, producerName, consumerName);
        }
    }

    // Client success

    public static void waitForClientSuccess(String namespaceName, String jobName, int messageCount) {
        waitForClientSuccess(namespaceName, jobName, messageCount, true);
    }

    public static void waitForClientSuccess(String namespaceName, String jobName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for client Job: {}/{} to finish successfully", namespaceName, jobName);
        TestUtils.waitFor("client Job to finish successfully", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> JobUtils.checkSucceededJobStatus(namespaceName, jobName, 1),
            () -> JobUtils.logCurrentJobStatus(namespaceName, jobName));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespaceName, jobName);
        }
    }

    // Client timeouts

    public static void waitForClientTimeout(String namespaceName, String jobName, int messageCount) {
        waitForClientTimeout(namespaceName, jobName, messageCount, true);
    }

    public static void waitForClientTimeout(String namespaceName, String jobName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for client Job: {}/{} to reach the timeout limit", namespaceName, jobName);
        try {
            TestUtils.waitFor("client Job: " + namespaceName + "/" + jobName + "to reach the timeout limit", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> JobUtils.checkFailedJobStatus(namespaceName, jobName, 1),
                () -> JobUtils.logCurrentJobStatus(namespaceName, jobName));

            if (deleteAfterSuccess) {
                JobUtils.deleteJobWithWait(namespaceName, jobName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Client Job: {}/{} reached the expected timeout", namespaceName, jobName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobWithWait(namespaceName, jobName);
                }
            } else {
                JobUtils.logCurrentJobStatus(namespaceName, jobName);
                throw e;
            }
        }
    }

    // Both clients timeouts

    public static void waitForClientsTimeout(String namespaceName, String consumerName, String producerName, int messageCount) {
        waitForClientsTimeout(namespaceName, consumerName, producerName, messageCount, true);
    }

    public static void waitForClientsTimeout(String namespaceName, String consumerName, String producerName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer {}/{} and consumer {}/{} Jobs to reach the timeout limit", namespaceName, producerName, namespaceName, consumerName);

        try {
            TestUtils.waitFor("client Jobs: " + producerName + " and " + consumerName + " in Namespace: " + namespaceName + " to reach the timeout limit", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> JobUtils.checkFailedJobStatus(namespaceName, producerName, 1)
                    && JobUtils.checkFailedJobStatus(namespaceName, consumerName, 1),
                () -> {
                    JobUtils.logCurrentJobStatus(namespaceName, producerName);
                    JobUtils.logCurrentJobStatus(namespaceName, consumerName);
                });

            if (deleteAfterSuccess) {
                JobUtils.deleteJobsWithWait(namespaceName, producerName, consumerName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Client Jobs {}/{} and {}/{} reached the expected timeout", namespaceName, producerName, namespaceName, consumerName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobsWithWait(namespaceName, producerName, consumerName);
                }
            } else {
                throw e;
            }
        }
    }

    private static long timeoutForClientFinishJob(int messagesCount) {
        // need to add at least 3 minutes for finishing the job (due to slow environments)
        return (long) messagesCount * 1000 + Duration.ofMinutes(3).toMillis();
    }

    /**
     * Method which generates random consumer group name
     * @return consumer group name with pattern: my-consumer-group-*-*
     */
    public static String generateRandomConsumerGroup() {
        int salt = RNG.nextInt(Integer.MAX_VALUE);

        return CONSUMER_GROUP_NAME + salt;
    }
}

