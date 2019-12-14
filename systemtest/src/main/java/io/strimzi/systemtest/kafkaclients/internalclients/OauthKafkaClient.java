/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalclients;

import io.strimzi.systemtest.kafkaclients.IKafkaClient;

import java.util.concurrent.Future;

public class OauthKafkaClient implements IKafkaClient {

    @Override
    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount) {
        return null;
    }

    @Override
    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String securityProtocol) {
        return null;
    }

    @Override
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount, String consumerGroup) {
        return null;
    }

    @Override
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String securityProtocol, String consumerGroup) {
        return null;
    }
}
