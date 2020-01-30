/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.kafkaclients.IKafkaClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;

/**
 * The TracingKafkaClient for sending and receiving messages using tracing properties.
 * The client is using an external listeners.
 */
public class TracingKafkaClient implements IKafkaClient<Future<Integer>> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClient.class);

    private String serviceName;

    public TracingKafkaClient() { }

    @Override
    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount,
                                        long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                           int messageCount, String securityProtocol, long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                           String consumerGroup, long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                              int messageCount, String securityProtocol, String consumerGroup, long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    public String getServiceName() {
        return this.serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
