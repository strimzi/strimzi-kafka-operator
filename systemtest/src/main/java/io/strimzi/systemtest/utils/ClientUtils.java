/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.IKafkaClient;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Random;

/**
 * ClientUtils class, which provides static methods for the all type clients
 * @see io.strimzi.systemtest.kafkaclients.externalClients.OauthKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.TracingKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.KafkaClient
 * @see io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient
 */
public class ClientUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClientUtils.class);

    // ensuring that object can not be created outside of class
    private ClientUtils() {}

    // TODO: all topicName, namespace, clusterName, userName should be removed after -> https://github.com/strimzi/strimzi-kafka-operator/pull/2520
    public static void waitUntilClientReceivedMessagesTls(IKafkaClient<Integer> kafkaClient, String topicName, String namespace,
                                                       String clusterName, String userName, int exceptedMessages) {
        TestUtils.waitFor("Kafka " + kafkaClient.toString() + " client received messages", Constants.GLOBAL_CLIENTS_POLL, Constants.GLOBAL_TIMEOUT,
            () -> {
                int receivedMessages = 0;
                try {
                    receivedMessages = kafkaClient.receiveMessagesTls(topicName, namespace, clusterName, userName,
                        exceptedMessages, "TLS", "group" + new Random().nextInt(Integer.MAX_VALUE),
                        Constants.GLOBAL_CLIENTS_TIMEOUT);
                    return receivedMessages == exceptedMessages;
                } catch (RuntimeException | IOException e) {
                    LOGGER.info("Client not received excepted messages {}, instead received only {}!", exceptedMessages, receivedMessages);
                    return false;
                }
            });
    }
}

