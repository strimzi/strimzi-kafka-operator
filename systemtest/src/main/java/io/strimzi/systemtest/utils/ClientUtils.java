/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.KafkaClientOperations;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * ClientUtils class, which provides static methods for the all type clients
 * @see io.strimzi.systemtest.kafkaclients.externalClients.OauthExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.TracingExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient
 */
public class ClientUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClientUtils.class);

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

    public static void waitTillContinuousClientsFinish(String producerName, String consumerName, String namespace, int messageCount) {
        long timeout = (long) messageCount * 3000;
        LOGGER.info("Waiting till producer {} and consumer {} finish for the following {} ms", producerName, consumerName, timeout);
        TestUtils.waitFor("continuous clients finished", Constants.GLOBAL_POLL_INTERVAL, timeout,
            () -> kubeClient().getClient().batch().jobs().inNamespace(namespace).withName(producerName).get().getStatus().getSucceeded().equals(1) &&
                    kubeClient().getClient().batch().jobs().inNamespace(namespace).withName(consumerName).get().getStatus().getSucceeded().equals(1));
    }

    public static void waitTillProducerOrConsumerFinish(String jobName, String namespace, int messageCount) {
        long timeout = (long) messageCount * 3000;
        LOGGER.info("Waiting for producer/consumer:{} will be finished in next {} ms", jobName, timeout);
        TestUtils.waitFor("job finished", Constants.GLOBAL_POLL_INTERVAL, timeout,
            () -> kubeClient().getClient().batch().jobs().inNamespace(namespace).withName(jobName).get().getStatus().getSucceeded().equals(1));
    }
}

