/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;


import java.io.IOException;

/**
 * Interface for unifying common methods of clients
 * @param <T> generic type which can be Future<Integer> or Integer depends on client type
 */
public interface IKafkaClient<T> {

    T sendMessages(String topicName, String namespace, String clusterName, int messageCount, long timeoutMs) throws IOException;
    T sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String securityProtocol, long timeoutMs) throws IOException;

    T receiveMessages(String topicName, String namespace, String clusterName, int messageCount, String consumerGroup, long timeoutMs) throws IOException;
    T receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String securityProtocol, String consumerGroup, long timeoutMs) throws IOException;
}
