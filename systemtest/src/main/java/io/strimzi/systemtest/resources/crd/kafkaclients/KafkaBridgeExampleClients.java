/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.HashMap;
import java.util.Map;

// HTTP Bridge clients
public class KafkaBridgeExampleClients extends KafkaBasicExampleClients {

    private final int port;
    private final int pollInterval;

    public static class Builder extends KafkaBasicExampleClients.Builder {
        private int port;
        private int pollInterval;

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withPollInterval(int pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        @Override
        public Builder withProducerName(String producerName) {
            return (Builder) super.withProducerName(producerName);
        }

        @Override
        public Builder withConsumerName(String consumerName) {
            return (Builder) super.withConsumerName(consumerName);
        }

        @Override
        public Builder withBootstrapAddress(String bootstrapAddress) {
            return (Builder) super.withBootstrapAddress(bootstrapAddress);
        }

        @Override
        public Builder withTopicName(String topicName) {
            return (Builder) super.withTopicName(topicName);
        }

        @Override
        public Builder withMessageCount(int messageCount) {
            return (Builder) super.withMessageCount(messageCount);
        }

        @Override
        public Builder withAdditionalConfig(String additionalConfig) {
            return (Builder) super.withAdditionalConfig(additionalConfig);
        }

        @Override
        public Builder withConsumerGroup(String consumerGroup) {
            return (Builder) super.withConsumerGroup(consumerGroup);
        }

        @Override
        public Builder withDelayMs(long delayMs) {
            return (Builder) super.withDelayMs(delayMs);
        }

        @Override
        public KafkaBridgeExampleClients build() {
            return new KafkaBridgeExampleClients(this);
        }
    }

    public int getPollInterval() {
        return pollInterval;
    }

    public int getPort() {
        return port;
    }

    protected Builder newBuilder() {
        return new Builder();
    }

    protected Builder updateBuilder(Builder builder) {
        super.updateBuilder(builder);
        return builder
            .withPort(getPort())
            .withPollInterval(getPollInterval());
    }

    public Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    private KafkaBridgeExampleClients(KafkaBridgeExampleClients.Builder builder) {
        super(builder);
        port = builder.port;
        pollInterval = builder.pollInterval;
    }


    public JobBuilder producerStrimziBridge() {
        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);
        producerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("OnFailure")
                        .withContainers()
                            .addNewContainer()
                                .withName(producerName)
                                .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.TEST_HTTP_PRODUCER_IMAGE)
                                .addNewEnv()
                                    .withName("HOSTNAME")
                                    .withValue(bootstrapAddress)
                                .endEnv()
                                .addNewEnv()
                                    .withName("PORT")
                                    .withValue(Integer.toString(port))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("DELAY_MS")
                                    .withValue(String.valueOf(delayMs))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(Integer.toString(messageCount))
                                .endEnv()
                            .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public JobBuilder consumerStrimziBridge() {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);
        consumerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(consumerLabels)
                .withName(consumerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("OnFailure")
                        .withContainers()
                            .addNewContainer()
                                .withName(consumerName)
                                .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.TEST_HTTP_CONSUMER_IMAGE)
                                .addNewEnv()
                                    .withName("HOSTNAME")
                                    .withValue(bootstrapAddress)
                                .endEnv()
                                .addNewEnv()
                                    .withName("PORT")
                                    .withValue(Integer.toString(port))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("POLL_INTERVAL")
                                    .withValue(Integer.toString(pollInterval))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(Integer.toString(messageCount))
                                .endEnv()
                                .addNewEnv()
                                    .withName("GROUP_ID")
                                    .withValue(consumerGroup)
                                .endEnv()
                            .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
