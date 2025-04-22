/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
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

    /**
     * Waits for both the instant producer and consumer clients to succeed, automatically deleting the associated jobs afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job and {@link TestStorage#getConsumerName()}
     * for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the clients' names.
     */
    public static void waitForInstantClientSuccess(TestStorage testStorage) {
        waitForInstantClientSuccess(testStorage, true);
    }

    /**
     * Waits for both the instant producer and consumer clients to succeed, optionally deleting jobs afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job and
     * {@link TestStorage#getConsumerName()} for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the clients' names.
     * @param deleteAfterSuccess Indicates whether jobs should be deleted after successful completion.
     */
    public static void waitForInstantClientSuccess(TestStorage testStorage, boolean deleteAfterSuccess) {
        waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount(), deleteAfterSuccess);
    }

    /**
     * Waits for both the continuous producer and consumer clients to succeed, automatically deleting the associated jobs afterward.
     * {@link TestStorage#getContinuousProducerName()} is used for identifying producer Job and
     * {@link TestStorage#getContinuousConsumerName()} for identifying consumer Job. The timeout while waiting is directly proportional
     * to the number of messages.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the clients' names.
     * @param messageCount The expected number of messages to be transmitted.
     */
    public static void waitForContinuousClientSuccess(TestStorage testStorage, int messageCount) {
        waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getContinuousConsumerName(), testStorage.getContinuousProducerName(), messageCount, true);
    }

    /**
     * Waits for both the continuous producer and consumer clients to succeed, with default number of messages expected to be transmitted.
     * {@link TestStorage#getContinuousProducerName()} is used for identifying producer Job and
     * {@link TestStorage#getContinuousConsumerName()} for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the clients' names.
     */
    public static void waitForContinuousClientSuccess(TestStorage testStorage) {
        waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getContinuousConsumerName(), testStorage.getContinuousProducerName(), testStorage.getContinuousMessageCount(), true);
    }

    public static void waitForClientsSuccess(String namespaceName, String consumerName, String producerName, int messageCount) {
        waitForClientsSuccess(namespaceName, consumerName, producerName, messageCount, true);
    }

    public static void waitForClientsSuccess(String namespaceName, String consumerName, String producerName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer: {}/{} and consumer: {}/{} Jobs to finish successfully", namespaceName, producerName, namespaceName, consumerName);
        TestUtils.waitFor("client Jobs to finish successfully", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> kubeClient().checkSucceededJobStatus(namespaceName, producerName, 1)
                && kubeClient().checkSucceededJobStatus(namespaceName, consumerName, 1),
            () -> {
                JobUtils.logCurrentJobStatus(namespaceName, producerName);
                JobUtils.logCurrentJobStatus(namespaceName, consumerName);
            });

        if (deleteAfterSuccess) {
            JobUtils.deleteJobsWithWait(namespaceName, producerName, consumerName);
        }
    }

    // Client success

    /**
     * Waits for the instant consumer client to succeed, automatically deleting the associated job afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job and
     * {@link TestStorage#getConsumerName()} for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the client's name.
     */
    public static void waitForInstantConsumerClientSuccess(TestStorage testStorage) {
        waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());
    }

    /**
     * Waits for the instant producer client to succeed with explicitly specified namespace automatically deleting the associated job afterward.
     *
     * @param namespaceName Explicit namespace name.
     * @param testStorage The {@link TestStorage} instance containing details about the client's name.
     */
    public static void waitForInstantProducerClientSuccess(String namespaceName, TestStorage testStorage) {
        waitForClientSuccess(namespaceName, testStorage.getProducerName(), testStorage.getMessageCount());
    }

    /**
     * Waits for the instant producer client to succeed, automatically deleting the associated job afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job and
     * {@link TestStorage#getConsumerName()} for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the client's name.
     */
    public static void waitForInstantProducerClientSuccess(TestStorage testStorage) {
        waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    public static void waitForClientSuccess(String namespaceName, String jobName, int messageCount) {
        waitForClientSuccess(namespaceName, jobName, messageCount, true);
    }

    public static void waitForClientSuccess(String namespaceName, String jobName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for client Job: {}/{} to finish successfully", namespaceName, jobName);
        TestUtils.waitFor("client Job to finish successfully", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> {
                LOGGER.debug("Client Job: {}/{} has status {}", namespaceName, jobName, kubeClient().namespace(namespaceName).getJobStatus(jobName));
                return kubeClient().checkSucceededJobStatus(namespaceName, jobName, 1);
            },
            () -> JobUtils.logCurrentJobStatus(namespaceName, jobName));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespaceName, jobName);
        }
    }

    // Client timeouts

    /**
     * Waits only for instant producer to timeout, automatically deleting the associated job afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job.
     *
     * @param testStorage The {@link TestStorage} instance containing details about the client's name.
     */
    public static void waitForInstantProducerClientTimeout(TestStorage testStorage) {
        waitForInstantProducerClientTimeout(testStorage, true);
    }

    /**
     * Waits only for instant producer to timeout, optionally deleting jobs afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job and
     *
     * @param testStorage The {@link TestStorage} instance contains details about client's name.
     * @param deleteAfterSuccess Indicates whether producer job should be deleted after timeout.
     */
    public static void waitForInstantProducerClientTimeout(TestStorage testStorage, boolean deleteAfterSuccess) {
        waitForClientTimeout(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount(), deleteAfterSuccess);
    }

    /**
     * Waits only for instant consumer to timeout, automatically deleting the associated job afterward.
     * {@link TestStorage#getConsumerName()} is used for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance contains details about client's name.
     */
    public static void waitForInstantConsumerClientTimeout(TestStorage testStorage) {
        waitForInstantConsumerClientTimeout(testStorage, true);
    }

    /**
     * Waits only for instant consumer to timeout, automatically deleting the associated job afterward.
     * {@link TestStorage#getConsumerName()} is used for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance contains details about client's name.
     * @param deleteAfterSuccess Indicates whether consumer job should be deleted after timeout.
     */
    public static void waitForInstantConsumerClientTimeout(TestStorage testStorage, boolean deleteAfterSuccess) {
        waitForClientTimeout(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount(), deleteAfterSuccess);
    }

    public static void waitForClientTimeout(String namespaceName, String jobName, int messageCount) {
        waitForClientTimeout(namespaceName, jobName, messageCount, true);
    }

    public static void waitForClientTimeout(String namespaceName, String jobName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for client Job: {}/{} to reach the timeout limit", namespaceName, jobName);
        try {
            TestUtils.waitFor("client Job: " + namespaceName + "/" + jobName + "to reach the the timeout limit", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespaceName, jobName, 1),
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

    /**
     * Waits only for instant consumer and producer to timeout, automatically deleting the associated jobs afterward.
     * {@link TestStorage#getProducerName()} is used for identifying producer Job and
     * {@link TestStorage#getConsumerName()} for identifying consumer Job.
     *
     * @param testStorage The {@link TestStorage} instance contains details about client's name.
     */
    public static void waitForInstantClientsTimeout(TestStorage testStorage) {
        waitForClientsTimeout(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    public static void waitForClientsTimeout(String namespaceName, String consumerName, String producerName, int messageCount) {
        waitForClientsTimeout(namespaceName, consumerName, producerName, messageCount, true);
    }

    public static void waitForClientsTimeout(String namespaceName, String consumerName, String producerName, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer {}/{} and consumer {}/{} Jobs to reach the timeout limit", namespaceName, producerName, namespaceName, consumerName);

        try {
            TestUtils.waitFor("client Jobs: " + producerName + " and " + consumerName + " in Namespace: " + namespaceName + " to reach the timeout limit", TestConstants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespaceName, producerName, 1)
                    && kubeClient().checkFailedJobStatus(namespaceName, consumerName, 1),
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

    public static void waitForClientContainsAllMessages(String namespaceName, String jobName, List<String> messages, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespaceName, jobName);
        List<String> notReadyMessages = messages;
        TestUtils.waitFor("client Job to contain all messages: [" + messages.toString() + "]", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.THROTTLING_EXCEPTION_TIMEOUT, () -> {
            for (String message : messages) {
                if (kubeClient().logsInSpecificNamespace(namespaceName, jobPodName).contains(message)) {
                    notReadyMessages.remove(message);
                }
            }

            if (deleteAfterSuccess && notReadyMessages.isEmpty()) {
                JobUtils.deleteJobWithWait(namespaceName, jobName);
            }

            return notReadyMessages.isEmpty();
        });
    }

    public static void waitForClientContainsMessage(String namespaceName, String jobName, String message) {
        waitForClientContainsMessage(namespaceName, jobName, message, true);
    }

    public static void waitForClientContainsMessage(String namespaceName, String jobName, String message, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespaceName, jobName);
        LOGGER.info("Waiting for client Job: {}/{} to contain message: [{}]", namespaceName, jobName, message);

        TestUtils.waitFor("client Job to contain message: [" + message + "]", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.THROTTLING_EXCEPTION_TIMEOUT,
            () -> kubeClient().logsInSpecificNamespace(namespaceName, jobPodName).contains(message),
            () -> JobUtils.logCurrentJobStatus(namespaceName, jobName));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespaceName, jobName);
        }
    }

    public static void waitForClientNotContainsMessage(String namespaceName, String jobName, String message) {
        waitForClientNotContainsMessage(namespaceName, jobName, message, true);
    }

    public static void waitForClientNotContainsMessage(String namespaceName, String jobName, String message, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespaceName, jobName);
        LOGGER.info("Waiting for client Job: {}/{} to not contain message: [{}]", namespaceName, jobName, message);

        TestUtils.waitFor("client Job to contain message: [" + message + "]", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.THROTTLING_EXCEPTION_TIMEOUT,
            () -> !kubeClient().logsInSpecificNamespace(namespaceName, jobPodName).contains(message),
            () -> JobUtils.logCurrentJobStatus(namespaceName, jobName));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespaceName, jobName);
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
        int salt = rng.nextInt(Integer.MAX_VALUE);

        return CONSUMER_GROUP_NAME + salt;
    }

    //////////////////////////////////
    // instant Plain client builders (ScramSha, TLS, plain)
    /////////////////////////////////

    /**
     * Creates and configures a {@link KafkaClientsBuilder} instance for instant Kafka clients based on test storage settings.
     * This base configuration sets up the namespace, message count, delay, topic name, producer name, and consumer name
     * for Kafka clients. {@link TestStorage#getProducerName()} is used for naming producer Job and
     * {@link TestStorage#getConsumerName()} for naming consumer Job. Finally, {@link TestStorage#getTopicName()}
     * is used as Topic target by attempted message transition. The default message count is set to 100, and the delay in milliseconds
     * is set to 0, indicating messages will be sent practically instantly. Returned builder can be modified as desired.
     *
     * @param testStorage The {@link TestStorage} instance containing configuration details
     *
     * @return A configured {@link KafkaClientsBuilder} instance ready for further customization or immediate use
     *         for creating Kafka producer and consumer clients.
     */
    private static KafkaClientsBuilder instantClientBuilderBase(TestStorage testStorage) {
        return new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount()) // default 100
            .withDelayMs(0)
            .withTopicName(testStorage.getTopicName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName());
    }

    // Instant ScramSha client builders

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using scram_sha over plain communication.
     * {@link TestStorage#getClusterName()} is with port 9092 is used to generate kafka bootstrap address.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with plain communication setup.
     */
    public static KafkaClientsBuilder getInstantScramShaOverPlainClientBuilder(TestStorage testStorage) {
        return getInstantScramShaClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
    }

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using scram_sha over tls communication.
     * {@link TestStorage#getClusterName()} is with port 9093 is used to generate kafka bootstrap address.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with tls communication setup.
     */
    public static KafkaClientsBuilder getInstantScramShaOverTlsClientBuilder(TestStorage testStorage) {
        return getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()));
    }

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using specified bootstrap (plain or TLS).
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @param bootstrapServer is the exact address including port (e.g., source-cluster-kafka-bootstrap:9095)
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with plain communication setup.
     */
    public static KafkaClientsBuilder getInstantScramShaClientBuilder(TestStorage testStorage, String bootstrapServer) {
        return getInstantPlainClientBuilder(testStorage, bootstrapServer)
            .withUsername(testStorage.getUsername());
    }

    // instant Plain client builders

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using plain communication (non-TLS).
     * {@link TestStorage#getClusterName()} and port 9092 are used to generate kafka bootstrap address.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with plain communication setup.
     */
    public static KafkaClientsBuilder getInstantPlainClientBuilder(TestStorage testStorage) {
        return getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
    }

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using plain communication (non-TLS),
     * extending the base configuration with the Kafka cluster's plain bootstrap address.
     * {@link TestStorage#getProducerName()} is used for naming producer Job and
     * {@link TestStorage#getConsumerName()} for naming consumer Job. Finally,
     * {@link TestStorage#getTopicName()} is used as Topic target by attempted message transition.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @param bootstrapServer is the exact address including port (e.g., source-cluster-kafka-bootstrap:9092)
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with plain communication setup.
     */
    public static KafkaClientsBuilder getInstantPlainClientBuilder(TestStorage testStorage, String bootstrapServer) {
        return instantClientBuilderBase(testStorage)
            .withBootstrapAddress(bootstrapServer);
    }

    // Instant TLS client builders

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using TLS communication.
     * {@link TestStorage#getClusterName()} and port 9093 are used to generate kafka bootstrap address.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with TLS communication setup.
     */
    public static KafkaClientsBuilder getInstantTlsClientBuilder(TestStorage testStorage) {
        return getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()));
    }

    /**
     * Generates a {@link KafkaClientsBuilder} for instant Kafka clients using TLS communication.
     * extending the base configuration with the Kafka cluster's plain bootstrap address.
     * {@link TestStorage#getProducerName()} is used for naming producer Job and
     * {@link TestStorage#getConsumerName()} for naming consumer Job. Finally,
     * {@link TestStorage#getTopicName()} is used as Topic target by attempted message transition.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @param bootstrapServer is the exact address including port (e.g., source-cluster-kafka-bootstrap:9093)
     * @return A configured {@link KafkaClientsBuilder} instance for instant clients with plain communication setup.
     */
    public static KafkaClientsBuilder getInstantTlsClientBuilder(TestStorage testStorage, String bootstrapServer) {
        return instantClientBuilderBase(testStorage)
            .withUsername(testStorage.getUsername())
            .withBootstrapAddress(bootstrapServer);
    }

    ////////////////////////////////////////////////////////////
    // (already build) instant clients (utilizing builders above)
    /////////////////////////////////////////////////////////////

    /**
     * Retrieves an instance of {@link KafkaClients} for plain communication with scramsha activated.
     * Targeting bootstrap address on port 9093 and leveraging the {@code getInstantPlainClientBuilder}
     * method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantScramShaOverPlainClients(TestStorage testStorage) {
        return getInstantScramShaOverPlainClientBuilder(testStorage).build();
    }

    /**
     * Retrieves an instance of {@link KafkaClients} for plain communication with scramsha activated.
     * Targeting bootstrap address on port 9093 and leveraging the {@code getInstantTlsClientBuilder}
     * method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantScramShaOverTlsClients(TestStorage testStorage) {
        return getInstantScramShaOverTlsClientBuilder(testStorage).build();
    }

    /**
     * Retrieves an instance of {@link KafkaClients} for plain communication with scramsha activated.
     * Leveraging the {@code getInstantPlainClientBuilder} method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @param bootstrapServer is the exact address including port (e.g., source-cluster-kafka-bootstrap:9096)
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantScramShaClients(TestStorage testStorage, String bootstrapServer) {
        return getInstantScramShaClientBuilder(testStorage, bootstrapServer).build();
    }

    /**
     * Retrieves an instance of {@link KafkaClients} for plain communication with Kafka brokers targeting port 9092 and,
     * leveraging the {@code getInstantPlainClientBuilder} method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantPlainClients(TestStorage testStorage) {
        return getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getClusterName())).build();
    }

    /**
     * Retrieves an instance of {@link KafkaClients} for plain communication with Kafka brokers,
     * leveraging the {@code getInstantPlainClientBuilder} method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @param bootstrapServer is the exact address including port (e.g., source-cluster-kafka-bootstrap:9092)
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantPlainClients(TestStorage testStorage, String bootstrapServer) {
        return getInstantPlainClientBuilder(testStorage, bootstrapServer).build();
    }

    /**
     * Retrieves an instance of {@link KafkaClients} for tls communication with Kafka brokers, targeting port 9093 and
     * leveraging the {@code getInstantTlsClientBuilder} method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantTlsClients(TestStorage testStorage) {
        return getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getClusterName())).build();
    }

    /**
     * Retrieves an instance of {@link KafkaClients} for tls communication with Kafka brokers,
     * leveraging the {@code getInstantTlsClientBuilder} method for initial configuration.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @param bootstrapServer is the exact address including port (e.g., source-cluster-kafka-bootstrap:9093)
     * @return build {@link KafkaClients}.
     */
    public static KafkaClients getInstantTlsClients(TestStorage testStorage, String bootstrapServer) {
        return getInstantTlsClientBuilder(testStorage, bootstrapServer).build();
    }

    //////////////////////////////////
    // continuous client builders
    /////////////////////////////////

    /**
     * Creates a {@link KafkaClientsBuilder} for continuous Kafka clients using plain (non-TLS) communication,
     * configuring it with properties specific to continuous operation scenarios. This includes setting up
     * (default 200 messages), a delay between messages (1000 ms) making ideal transition last by default for around 3-4 minutes.
     * {@link TestStorage#getContinuousProducerName()}  is used for naming producer Job and
     * {@link TestStorage#getContinuousConsumerName()}  for naming consumer Job. Finally,
     * {@link TestStorage#getContinuousTopicName()}  is used as Topic target by attempted message transition.
     *
     * @param testStorage The {@link TestStorage} instance providing necessary configurations.
     * @return A configured {@link KafkaClientsBuilder} instance ready for creating Kafka clients for continuous
     * operations with plain communication, ready for further customization.
     */
    public static KafkaClientsBuilder getContinuousPlainClientBuilder(TestStorage testStorage) {
        return new KafkaClientsBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getContinuousMessageCount()) // default 200
            .withDelayMs(1000)
            .withTopicName(testStorage.getContinuousTopicName())
            .withProducerName(testStorage.getContinuousProducerName())
            .withConsumerName(testStorage.getContinuousConsumerName());
    }

}

