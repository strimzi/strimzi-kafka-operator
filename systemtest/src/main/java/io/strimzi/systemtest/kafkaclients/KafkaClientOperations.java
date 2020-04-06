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
 * @param <T> returned type of each operation
 */
public interface KafkaClientOperations<T> {

    /**
     * Sending plain messages with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return <T> type
     * @throws Exception exception
     */
    T sendMessagesPlain(long timeoutMs) throws Exception;

    /**
     * Sending encrypted messages using Tls technology with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return <T> type
     * @throws Exception exception
     */
    T sendMessagesTls(long timeoutMs) throws Exception;

    /**
     * Receiving plain messages with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return <T> type
     * @throws Exception exception
     */

    T receiveMessagesPlain(long timeoutMs) throws Exception;

    /**
     * Sending encrypted messages using Tls technology with the selected client
     * @param timeoutMs timeout in milliseconds
     * @return <T> type
     * @throws Exception exception
     */
    T receiveMessagesTls(long timeoutMs) throws Exception;
}
