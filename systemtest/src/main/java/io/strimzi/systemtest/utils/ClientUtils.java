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

    public static void waitUntilClientReceivedMessagesTls(KafkaClientOperations kafkaClient, int exceptedMessages) {
        TestUtils.waitFor("Kafka " + kafkaClient.toString() + " client received messages", Constants.GLOBAL_CLIENTS_POLL, Constants.GLOBAL_TIMEOUT,
            () -> {
                int receivedMessages = 0;
                try {
                    receivedMessages = kafkaClient.receiveMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);
                    return receivedMessages == exceptedMessages;
                } catch (Exception e) {
                    LOGGER.info("Client not received excepted messages {}, instead received only {}!", exceptedMessages, receivedMessages);
                    return false;
                }
            });
    }
}

