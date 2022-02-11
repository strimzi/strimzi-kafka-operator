/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    protected String namespaceName;

    public static class Builder<SELF extends Builder<SELF>> {
        private String producerName;
        private String consumerName;
        private String bootstrapAddress;
        private String topicName;
        private String message;
        private int messageCount;
        private String additionalConfig;
        private String consumerGroup;
        private long delayMs;
        private String namespaceName;

        public SELF withProducerName(String producerName) {
            this.producerName = producerName;
            return self();
        }

        public SELF withConsumerName(String consumerName) {
            this.consumerName = consumerName;
            return self();
        }

        public SELF withBootstrapAddress(String bootstrapAddress) {
            this.bootstrapAddress = bootstrapAddress;
            return self();
        }

        public SELF withTopicName(String topicName) {
            this.topicName = topicName;
            return self();
        }

        public SELF withMessageCount(int messageCount) {
            this.messageCount = messageCount;
            return self();
        }

        public SELF withMessage(String message) {
            this.message = message;
            return self();
        }

        public SELF withAdditionalConfig(String additionalConfig) {
            this.additionalConfig = additionalConfig;
            return self();
        }

        public SELF withConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return self();
        }

        public SELF withDelayMs(long delayMs) {
            this.delayMs = delayMs;
            return self();
        }

        public SELF withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return self();
        }

        @SuppressWarnings("unchecked")
        protected SELF self() {
            return (SELF) this;
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
        if (builder.additionalConfig == null || builder.additionalConfig.isEmpty()) builder.additionalConfig = "";

        producerName = builder.producerName;
        consumerName = builder.consumerName;
        bootstrapAddress = builder.bootstrapAddress;
        topicName = builder.topicName;
        message = builder.message;
        messageCount = builder.messageCount;
        additionalConfig = builder.additionalConfig;
        consumerGroup = builder.consumerGroup;
        delayMs = builder.delayMs;
        namespaceName = builder.namespaceName;
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

    public String getNamespaceName() {
        return namespaceName;
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
            .withMessage(getMessage())
            .withNamespaceName(getNamespaceName());
    }

    public Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    public JobBuilder producerStrimzi() {
        return defaultProducerStrimzi();
    }

    public JobBuilder producerScramShaStrimzi(final String clusterName, final String kafkaUserName) {
        // fetch secret
        final String saslJaasConfigEncrypted = ResourceManager.kubeClient().getSecret(namespaceName, kafkaUserName).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);

        additionalConfig +=
            // scram-sha
            "ssl.endpoint.identification.algorithm=\n" +
                "sasl.mechanism=SCRAM-SHA-512\n" +
                "security.protocol=" + SecurityProtocol.SASL_SSL + "\n" +
                "sasl.jaas.config=" + saslJaasConfigDecrypted;

        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(getClusterCaCertEnv(clusterName))
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public JobBuilder producerTlsStrimzi(final String clusterName, final String kafkaUserName) {
        additionalConfig +=
            // tls
            "ssl.endpoint.identification.algorithm=\n" +
            "sasl.mechanism=GSSAPI\n" +
            "security.protocol=" + SecurityProtocol.SSL + "\n";

        EnvVar userCrt = new EnvVarBuilder()
            .withName("USER_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(kafkaUserName)
                    .withKey("user.crt")
                .endSecretKeyRef()
            .endValueFrom()
            .build();

        EnvVar userKey = new EnvVarBuilder()
            .withName("USER_KEY")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(kafkaUserName)
                    .withKey("user.key")
                .endSecretKeyRef()
            .endValueFrom()
            .build();

        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(getClusterCaCertEnv(clusterName), userCrt, userKey)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }


    public JobBuilder consumerStrimzi() {
        return defaultConsumerStrimzi();
    }

    public JobBuilder defaultProducerStrimzi() {
        if (producerName == null || producerName.isEmpty()) throw new InvalidParameterException("Producer name is not set.");
        if (namespaceName == null || namespaceName.isEmpty()) {
            LOGGER.info("Deploy {} to namespace: {}", producerName, ResourceManager.kubeClient().getNamespace());
            namespaceName = ResourceManager.kubeClient().getNamespace();
        }

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);
        producerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(namespaceName)
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(producerName)
                        .withNamespace(namespaceName)
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                        .withContainers()
                            .addNewContainer()
                                .withName(producerName)
                                .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.TEST_PRODUCER_IMAGE)
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
        if (namespaceName == null || namespaceName.isEmpty()) {
            LOGGER.info("Deploy {} to namespace: {}", consumerName, ResourceManager.kubeClient().getNamespace());
            namespaceName = ResourceManager.kubeClient().getNamespace();
        }

        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);
        consumerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(namespaceName)
                .withLabels(consumerLabels)
                .withName(consumerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                        .withNamespace(namespaceName)
                        .withName(consumerName)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                            .withContainers()
                                .addNewContainer()
                                    .withName(consumerName)
                                    .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                    .withImage(Environment.TEST_CONSUMER_IMAGE)
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

    protected EnvVar getClusterCaCertEnv(String clusterName) {
        return new EnvVarBuilder()
            .withName("CA_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                    .withKey("ca.crt")
                .endSecretKeyRef()
            .endValueFrom()
            .build();
    }
}
