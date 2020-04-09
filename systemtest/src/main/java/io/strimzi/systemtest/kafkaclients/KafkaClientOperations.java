/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

/**
 * Interface KafkaClientOperations used for basic operations with the clients
 *
 * @see io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.OauthExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.TracingExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient
 */
public interface KafkaClientOperations {

    /**
     * Sending plain messages with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return count of messages
     * @throws Exception exception
     */
    int sendMessagesPlain(long timeoutMs) throws Exception;

    /**
     * Sending encrypted messages using Tls technology with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return count of messages
     * @throws Exception exception
     */
    int sendMessagesTls(long timeoutMs) throws Exception;

    /**
     * Receiving plain messages with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return count of messages
     * @throws Exception exception
     */

    int receiveMessagesPlain(long timeoutMs) throws Exception;

    /**
     * Sending encrypted messages using Tls technology with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return count of messages
     * @throws Exception exception
     */
    int receiveMessagesTls(long timeoutMs) throws Exception;
}
