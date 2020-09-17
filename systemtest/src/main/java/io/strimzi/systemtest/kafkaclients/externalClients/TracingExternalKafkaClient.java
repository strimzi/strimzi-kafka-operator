/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.kafkaclients.AbstractKafkaClient;
import io.strimzi.systemtest.kafkaclients.KafkaClientOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The TracingKafkaClient for sending and receiving messages using tracing properties.
 * The client is using an external listeners.
 */
public class TracingExternalKafkaClient extends AbstractKafkaClient<TracingExternalKafkaClient> implements KafkaClientOperations {

    private static final Logger LOGGER = LogManager.getLogger(TracingExternalKafkaClient.class);
    private String serviceName;

    public static class Builder extends AbstractKafkaClient.Builder<TracingExternalKafkaClient.Builder> {

        private String serviceName;

        public Builder withServiceName(String serviceName) {

            this.serviceName = serviceName;
            return self();
        }

        @Override
        public TracingExternalKafkaClient build() {

            return new TracingExternalKafkaClient(this);
        }

        @Override
        protected TracingExternalKafkaClient.Builder self() {
            return this;
        }
    }

    @Override
    protected AbstractKafkaClient.Builder<TracingExternalKafkaClient.Builder> toBuilder(TracingExternalKafkaClient tracingExternalKafkaClient) {
        TracingExternalKafkaClient.Builder builder = new TracingExternalKafkaClient.Builder();

        builder.withTopicName(tracingExternalKafkaClient.getTopicName());
        builder.withPartition(tracingExternalKafkaClient.getPartition());
        builder.withMessageCount(tracingExternalKafkaClient.getMessageCount());
        builder.withNamespaceName(tracingExternalKafkaClient.getNamespaceName());
        builder.withClusterName(tracingExternalKafkaClient.getClusterName());
        builder.withConsumerGroupName(tracingExternalKafkaClient.getConsumerGroup());
        builder.withKafkaUsername(tracingExternalKafkaClient.getKafkaUsername());
        builder.withSecurityProtocol(tracingExternalKafkaClient.getSecurityProtocol());
        builder.withCertificateAuthorityCertificateName(tracingExternalKafkaClient.getCaCertName());
        builder.withProducerProperties(tracingExternalKafkaClient.getProducerProperties());
        builder.withConsumerProperties(tracingExternalKafkaClient.getConsumerProperties());
        builder.withServiceName(tracingExternalKafkaClient.getServiceName());

        return builder;
    }
    private TracingExternalKafkaClient(TracingExternalKafkaClient.Builder builder) {

        super(builder);
        serviceName = builder.serviceName;
    }

    // TODO: these methods will be implemented in the following PR for the clients with support of tracing
    @Override
    public int sendMessagesPlain(long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int sendMessagesTls(long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int receiveMessagesPlain(long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int receiveMessagesTls(long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String toString() {
        return "TracingKafkaClient{" +
                "serviceName='" + serviceName + '\'' +
                ", topicName='" + topicName + '\'' +
                ", namespaceName='" + namespaceName + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", messageCount=" + messageCount +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", kafkaUsername='" + kafkaUsername + '\'' +
                ", securityProtocol='" + securityProtocol + '\'' +
                ", caCertName='" + caCertName + '\'' +
                '}';
    }
}
