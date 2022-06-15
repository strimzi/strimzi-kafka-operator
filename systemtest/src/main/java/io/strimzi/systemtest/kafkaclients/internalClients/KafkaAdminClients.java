/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.sundr.builder.annotations.Buildable;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Buildable(editableEnabled = false)
public class KafkaAdminClients extends BaseClients {
    private int partitions;
    private int replicationFactor;
    private AdminClientOperation adminOperation;
    private String adminName;
    private int topicCount;
    private int topicOffset;
    private String topicName;

    public String getAdminName() {
        return adminName;
    }

    public void setAdminName(String adminName) {
        this.adminName = adminName;
    }

    public int getTopicCount() {
        return topicCount;
    }

    public void setTopicCount(int topicCount) {
        this.topicCount = topicCount <= 0 ? 1 : topicCount;
    }

    public int getTopicOffset() {
        return topicOffset;
    }

    public void setTopicOffset(int topicOffset) {
        this.topicOffset = topicOffset;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions <= 0 ? 1 : partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor <= 0 ? 1 : replicationFactor;
    }

    public AdminClientOperation getAdminOperation() {
        return adminOperation;
    }

    public void setAdminOperation(AdminClientOperation adminOperation) {
        if (adminOperation == null) {
            throw new InvalidParameterException("TopicOperation must be set.");
        }

        this.adminOperation = adminOperation;
    }

    @Override
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String getTopicName() {
        return this.topicName;
    }

    public static String getAdminClientScramConfig(String namespace, String kafkaUsername, int timeoutMs) {
        final String saslJaasConfigEncrypted = kubeClient().getSecret(namespace, kafkaUsername).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);
        return
            "request.timeout.ms=" + timeoutMs + "\n" +
            "sasl.mechanism=SCRAM-SHA-512\n" +
            "security.protocol=" + SecurityProtocol.SASL_PLAINTEXT + "\n" +
            "sasl.jaas.config=" + saslJaasConfigDecrypted;
    }

    public Job defaultAdmin() {
        if ((this.getTopicName() == null || this.getTopicName().isEmpty())
            && !(this.getAdminOperation().equals(AdminClientOperation.HELP) || this.getAdminOperation().equals(AdminClientOperation.LIST_TOPICS))) {
            throw new InvalidParameterException("Topic name (or 'prefix' if topic count > 1) is not set.");
        }

        Map<String, String> adminLabels = new HashMap<>();
        adminLabels.put("app", adminName);
        adminLabels.put(Constants.KAFKA_ADMIN_CLIENT_LABEL_KEY, Constants.KAFKA_ADMIN_CLIENT_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(this.getNamespaceName())
                .withLabels(adminLabels)
                .withName(this.getAdminName())
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(this.getAdminName())
                        .withNamespace(this.getNamespaceName())
                        .withLabels(adminLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                            .withContainers()
                                .addNewContainer()
                                .withName(this.getAdminName())
                                .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.TEST_ADMIN_IMAGE)
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(this.getBootstrapAddress())
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(this.getTopicName())
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC_OPERATION")
                                    .withValue(this.getAdminOperation().toString())
                                .endEnv()
                                .addNewEnv()
                                    .withName("REPLICATION_FACTOR")
                                    .withValue(String.valueOf(this.getReplicationFactor()))
                                .endEnv()
                                .addNewEnv()
                                    .withName("PARTITIONS")
                                    .withValue(String.valueOf(this.getPartitions()))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPICS_COUNT")
                                    .withValue(String.valueOf(this.getTopicCount()))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC_OFFSET")
                                    .withValue(String.valueOf(this.getTopicOffset()))
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("ADDITIONAL_CONFIG")
                                    .withValue(this.getAdditionalConfig())
                                .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }
}
