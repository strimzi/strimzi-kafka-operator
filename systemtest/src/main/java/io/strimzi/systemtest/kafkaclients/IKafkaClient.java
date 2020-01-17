/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

public interface IKafkaClient<T> {

    T sendMessages(String topicName, String namespace, String clusterName, int messageCount) throws InterruptedException;
    T sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String securityProtocol) throws InterruptedException;

    T receiveMessages(String topicName, String namespace, String clusterName, int messageCount, String consumerGroup);
    T receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String securityProtocol, String consumerGroup);
}
