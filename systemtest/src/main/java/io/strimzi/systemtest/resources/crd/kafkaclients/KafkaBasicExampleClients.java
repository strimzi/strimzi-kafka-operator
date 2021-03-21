/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

public class KafkaBasicExampleClients {

    private static final Logger LOGGER = LogManager.getLogger(KafkaBasicExampleClients.class);

    protected String producerName;
    protected String consumerName;
    protected String bootstrapAddress;
    protected String topicName;
    protected String message;
    protected int messageCount;
    protected String additionalConfig;
    protected String consumerGroup;
    protected long delayMs;

    public static class Builder {
        private String producerName;
        private String consumerName;
        private String bootstrapAddress;
        private String topicName;
        private String message;
        private int messageCount;
        private String additionalConfig;
        private String consumerGroup;
        private long delayMs;

        public Builder withProducerName(String producerName) {
            this.producerName = producerName;
            return this;
        }

        public Builder withConsumerName(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder withBootstrapAddress(String bootstrapAddress) {
            this.bootstrapAddress = bootstrapAddress;
            return this;
        }

        public Builder withTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder withMessageCount(int messageCount) {
            this.messageCount = messageCount;
            return this;
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder withAdditionalConfig(String additionalConfig) {
            this.additionalConfig = additionalConfig;
            return this;
        }

        public Builder withConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }

        public Builder withDelayMs(long delayMs) {
            this.delayMs = delayMs;
            return this;
        }

        public KafkaBasicExampleClients build() {
            return new KafkaBasicExampleClients(this);
        }
    }

    protected KafkaBasicExampleClients(Builder builder) {
        if (builder.topicName == null || builder.topicName.isEmpty()) throw new InvalidParameterException("Topic name is not set.");
        if (builder.bootstrapAddress == null || builder.bootstrapAddress.isEmpty()) throw new InvalidParameterException("Bootstrap server is not set.");
        if (builder.messageCount <= 0) throw  new InvalidParameterException("Message count is less than 1");
        if (builder.consumerGroup == null || builder.consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            builder.consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }
        if (builder.message == null || builder.message.isEmpty()) builder.message = "Hello-world";



        producerName = builder.producerName;
        consumerName = builder.consumerName;
        bootstrapAddress = builder.bootstrapAddress;
        topicName = builder.topicName;
        message = builder.message;
        messageCount = builder.messageCount;
        additionalConfig = builder.additionalConfig;
        consumerGroup = builder.consumerGroup;
        delayMs = builder.delayMs;
    }

    public String getProducerName() {
        return producerName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getBootstrapAddress() {
        return bootstrapAddress;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getMessage() {
        return message;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public String getAdditionalConfig() {
        return additionalConfig;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public long getDelayMs() {
        return delayMs;
    }

    protected Builder newBuilder() {
        return new Builder();
    }

    protected Builder updateBuilder(Builder builder) {
        return builder
            .withConsumerGroup(getConsumerGroup())
            .withAdditionalConfig(getAdditionalConfig())
            .withBootstrapAddress(getBootstrapAddress())
            .withConsumerName(getConsumerName())
            .withDelayMs(getDelayMs())
            .withMessageCount(getMessageCount())
            .withProducerName(getProducerName())
            .withTopicName(getTopicName())
            .withMessage(getMessage());
    }

    public Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    public JobBuilder producerStrimzi() {
        return defaultProducerStrimzi();
    }

    public JobBuilder consumerStrimzi() {
        return defaultConsumerStrimzi();
    }

    public JobBuilder defaultProducerStrimzi() {
        if (producerName == null || producerName.isEmpty()) throw new InvalidParameterException("Producer name is not set.");

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);
        producerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

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
                        .withName(producerName)
                        .withNamespace(ResourceManager.kubeClient().getNamespace())
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("Never")
                        .withContainers()
                            .addNewContainer()
                                .withName(producerName)
                                .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.STRIMZI_REGISTRY_DEFAULT + "/" + Environment.STRIMZI_CLIENTS_ORG_DEFAULT + "/" + Constants.STRIMZI_EXAMPLE_PRODUCER_NAME + ":latest")
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrapAddress)
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
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(String.valueOf(messageCount))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE")
                                    .withValue(message)
                                .endEnv()
                                .addNewEnv()
                                    .withName("PRODUCER_ACKS")
                                    .withValue("all")
                                .endEnv()
                                .addNewEnv()
                                    .withName("ADDITIONAL_CONFIG")
                                    .withValue(additionalConfig)
                                .endEnv()
                                .addNewEnv()
                                    .withName("BLOCKING_PRODUCER")
                                    .withValue("true")
                                .endEnv()
                            .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public JobBuilder defaultConsumerStrimzi() {
        if (consumerName == null || consumerName.isEmpty()) throw new InvalidParameterException("Consumer name is not set.");

        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);
        consumerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

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
                        .withRestartPolicy("Never")
                            .withContainers()
                                .addNewContainer()
                                    .withName(consumerName)
                                    .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                    .withImage(Environment.STRIMZI_REGISTRY_DEFAULT + "/" + Environment.STRIMZI_CLIENTS_ORG_DEFAULT + "/" + Constants.STRIMZI_EXAMPLE_CONSUMER_NAME + ":latest")
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapAddress)
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
                                        .withName("LOG_LEVEL")
                                        .withValue("DEBUG")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MESSAGE_COUNT")
                                        .withValue(String.valueOf(messageCount))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("GROUP_ID")
                                        .withValue(consumerGroup)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("ADDITIONAL_CONFIG")
                                        .withValue(additionalConfig)
                                    .endEnv()
                                .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
