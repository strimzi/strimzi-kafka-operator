/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.ClientUtils;
import io.sundr.builder.annotations.Buildable;
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

@Buildable(editableEnabled = false)
public class KafkaClients extends BaseClients {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClients.class);

    private String producerName;
    private String consumerName;
    private String message;
    private int messageCount;
    private String consumerGroup;
    private long delayMs;

    public String getProducerName() {
        return producerName;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        if (message == null || message.isEmpty()) {
            message = "Hello-world";
        }
        this.message = message;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        if (messageCount <= 0) {
            throw new InvalidParameterException("Message count is less than 1");
        }
        this.messageCount = messageCount;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }
        this.consumerGroup = consumerGroup;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public Job producerStrimzi() {
        return defaultProducerStrimzi().build();
    }

    public Job producerScramShaStrimzi(final String clusterName, final String kafkaUserName) {
        // fetch secret
        final String saslJaasConfigEncrypted = ResourceManager.kubeClient().getSecret(this.getNamespaceName(), kafkaUserName).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);

        this.setAdditionalConfig(this.getAdditionalConfig() +
            // scram-sha
            "ssl.endpoint.identification.algorithm=\n" +
                "sasl.mechanism=SCRAM-SHA-512\n" +
                "security.protocol=" + SecurityProtocol.SASL_SSL + "\n" +
                "sasl.jaas.config=" + saslJaasConfigDecrypted);

        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(getClusterCaCertEnv(clusterName))
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job producerTlsStrimzi(final String clusterName, final String kafkaUserName) {
        this.setAdditionalConfig(this.getAdditionalConfig() +
            "ssl.endpoint.identification.algorithm=\n" +
            "sasl.mechanism=GSSAPI\n" +
            "security.protocol=" + SecurityProtocol.SSL + "\n");

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
            .endSpec()
            .build();
    }


    public Job consumerStrimzi() {
        return defaultConsumerStrimzi().build();
    }

    public JobBuilder defaultProducerStrimzi() {
        if (producerName == null || producerName.isEmpty()) {
            throw new InvalidParameterException("Producer name is not set.");
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
                .withNamespace(this.getNamespaceName())
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(producerName)
                        .withNamespace(this.getNamespaceName())
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
                                    .withValue(this.getBootstrapAddress())
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(this.getTopicName())
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
                                    .withValue(this.getAdditionalConfig())
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
        if (consumerName == null || consumerName.isEmpty()) {
            throw new InvalidParameterException("Consumer name is not set.");
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
                .withNamespace(this.getNamespaceName())
                .withLabels(consumerLabels)
                .withName(consumerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                        .withNamespace(this.getNamespaceName())
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
                                        .withValue(this.getBootstrapAddress())
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue(this.getTopicName())
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
                                        .withValue(this.getAdditionalConfig())
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
