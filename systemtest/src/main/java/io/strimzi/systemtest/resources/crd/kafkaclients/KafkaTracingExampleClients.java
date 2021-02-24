/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.HashMap;
import java.util.Map;

public class KafkaTracingExampleClients extends KafkaBasicExampleClients {

    private static final String JAEGER_SAMPLER_TYPE =  "const";
    private static final String JAEGER_SAMPLER_PARAM =  "1";

    private final String jaegerServiceProducerName;
    private final String jaegerServiceConsumerName;
    private final String jaegerServiceStreamsName;
    private final String jaegerServerAgentName;
    private final String streamsTopicTargetName;

    public static class Builder extends KafkaBasicExampleClients.Builder {
        private String jaegerServiceProducerName;
        private String jaegerServiceConsumerName;
        private String jaegerServiceStreamsName;
        private String jaegerServerAgentName;
        private String streamsTopicTargetName;

        public Builder withJaegerServiceProducerName(String jaegerServiceProducerName) {
            this.jaegerServiceProducerName = jaegerServiceProducerName;
            return this;
        }

        public Builder withJaegerServiceConsumerName(String jaegerServiceConsumerName) {
            this.jaegerServiceConsumerName = jaegerServiceConsumerName;
            return this;
        }

        public Builder withJaegerServiceStreamsName(String jaegerServiceStreamsName) {
            this.jaegerServiceStreamsName = jaegerServiceStreamsName;
            return this;
        }

        public Builder withJaegerServiceAgentName(String jaegerServerAgentName) {
            this.jaegerServerAgentName = jaegerServerAgentName;
            return this;
        }

        public Builder withStreamsTopicTargetName(String streamsTopicTargetName) {
            this.streamsTopicTargetName = streamsTopicTargetName;
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
        public KafkaTracingExampleClients build() {
            return new KafkaTracingExampleClients(this);
        }
    }

    public String getJaegerServiceConsumerName() {
        return jaegerServiceConsumerName;
    }

    public String getJaegerServiceProducerName() {
        return jaegerServiceProducerName;
    }

    public String getJaegerServiceStreamsName() {
        return jaegerServiceStreamsName;
    }

    public String getJaegerServiceAgentName() {
        return jaegerServerAgentName;
    }

    public String getStreamsTopicTargetName() {
        return streamsTopicTargetName;
    }

    protected Builder newBuilder() {
        return new Builder();
    }

    protected Builder updateBuilder(Builder builder) {
        super.updateBuilder(builder);
        return builder
            .withJaegerServiceProducerName(getJaegerServiceProducerName())
            .withJaegerServiceConsumerName(getJaegerServiceConsumerName())
            .withJaegerServiceStreamsName(getJaegerServiceStreamsName())
            .withJaegerServiceAgentName(getJaegerServiceAgentName())
            .withStreamsTopicTargetName(getStreamsTopicTargetName());
    }

    public Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    public KafkaTracingExampleClients(KafkaTracingExampleClients.Builder builder) {
        super(builder);
        jaegerServiceProducerName = builder.jaegerServiceProducerName;
        jaegerServiceConsumerName = builder.jaegerServiceConsumerName;
        jaegerServiceStreamsName = builder.jaegerServiceStreamsName;
        jaegerServerAgentName = builder.jaegerServerAgentName;
        streamsTopicTargetName = builder.streamsTopicTargetName;
    }

    public JobBuilder consumerWithTracing() {
        return defaultConsumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(jaegerServiceConsumerName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(jaegerServerAgentName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public JobBuilder producerWithTracing() {
        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(jaegerServiceProducerName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(jaegerServerAgentName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public JobBuilder kafkaStreamsWithTracing() {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamLabels = new HashMap<>();
        kafkaStreamLabels.put("app", kafkaStreamsName);

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(kafkaStreamLabels)
                .withName(kafkaStreamsName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(kafkaStreamLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("Never")
                        .withContainers()
                        .addNewContainer()
                            .withName(kafkaStreamsName)
                            .withImage(Environment.STRIMZI_REGISTRY_DEFAULT + "/" + Environment.STRIMZI_CLIENTS_ORG_DEFAULT + "/" + Constants.STRIMZI_EXAMPLE_STREAMS_NAME + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrapAddress)
                              .endEnv()
                            .addNewEnv()
                                .withName("APPLICATION_ID")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("SOURCE_TOPIC")
                                .withValue(topicName)
                            .endEnv()
                            .addNewEnv()
                                .withName("TARGET_TOPIC")
                                .withValue(streamsTopicTargetName)
                            .endEnv()
                              .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("DEBUG")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(jaegerServiceStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(jaegerServerAgentName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
