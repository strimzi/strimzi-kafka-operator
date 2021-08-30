/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

/**
 * KafkaAdmin Client - initial implementation of test-client,
 * which supports Topic management operations only.
 * Create, delete, update topics and partitions increase (in bulk, with offsets).
 */
public class KafkaAdminClient {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminClient.class);

    protected String adminName;
    protected String bootstrapAddress;
    protected String topicName;
    protected int topicCount;
    protected int topicOffset;
    protected int partitions;
    protected int replicationFactor;
    protected String topicOperation;
    protected String additionalConfig;
    protected String namespaceName;

    protected KafkaAdminClient(KafkaAdminClient.Builder builder) {
        if (builder.topicOperation == null || builder.topicOperation.isEmpty())
            throw new InvalidParameterException("TopicOperation must be set.");
        if (builder.bootstrapAddress == null || builder.bootstrapAddress.isEmpty())
            throw new InvalidParameterException("Bootstrap server is not set.");
        if ((builder.topicName == null || builder.topicName.isEmpty()) && !(builder.topicOperation.equals("help") || builder.topicOperation.equals("list")))
            throw new InvalidParameterException("Topic name (or 'prefix' if topic count > 1) is not set.");

        replicationFactor = (builder.replicationFactor == 0) ? 1 : builder.replicationFactor;
        partitions = (builder.partitions == 0) ? 1 : builder.partitions;
        topicCount = (builder.topicCount == 0) ? 1 : builder.topicCount;
        topicOffset = builder.topicOffset;
        adminName = builder.adminName;
        bootstrapAddress = builder.bootstrapAddress;
        topicName = builder.topicName;
        additionalConfig = builder.additionalConfig;
        namespaceName = builder.namespaceName;
        topicOperation = builder.topicOperation;
    }

    public String getAdminName() {
        return adminName;
    }

    public String getBootstrapAddress() {
        return bootstrapAddress;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getTopicCount() {
        return topicCount;
    }

    public int getTopicOffset() {
        return topicOffset;
    }

    public String getAdditionalConfig() {
        return additionalConfig;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public int getPartitions() {
        return partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getTopicOperation() {
        return topicOperation;
    }

    protected KafkaAdminClient.Builder newBuilder() {
        return new KafkaAdminClient.Builder();
    }

    protected KafkaAdminClient.Builder updateBuilder(KafkaAdminClient.Builder builder) {
        return builder
                .withAdditionalConfig(getAdditionalConfig())
                .withBootstrapAddress(getBootstrapAddress())
                .withTopicOperation(getTopicOperation())
                .withTopicCount(getTopicCount())
                .withTopicOffset(getTopicOffset())
                .withPartitions(getPartitions())
                .withReplicationFactor(getReplicationFactor())
                .withAdminName(getAdminName())
                .withTopicName(getTopicName())
                .withNamespaceName(getNamespaceName());
    }

    public KafkaAdminClient.Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    public JobBuilder defaultAdmin() {
        if (namespaceName == null || namespaceName.isEmpty()) {
            LOGGER.info("Deploying {} to namespace: {}", adminName, ResourceManager.kubeClient().getNamespace());
            namespaceName = ResourceManager.kubeClient().getNamespace();
        }

        Map<String, String> adminLabels = new HashMap<>();
        adminLabels.put("app", adminName);
        adminLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

        return new JobBuilder()
                .withNewMetadata()
                    .withNamespace(namespaceName)
                    .withLabels(adminLabels)
                    .withName(adminName)
                .endMetadata()
                .withNewSpec()
                    .withBackoffLimit(0)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withName(adminName)
                            .withNamespace(namespaceName)
                            .withLabels(adminLabels)
                        .endMetadata()
                        .withNewSpec()
                            .withRestartPolicy("Never")
                                .withContainers()
                                    .addNewContainer()
                                    .withName(adminName)
                                    .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                    .withImage(Environment.TEST_ADMIN_IMAGE)
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapAddress)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue(topicName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC_OPERATION")
                                        .withValue(topicOperation)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("REPLICATION_FACTOR")
                                        .withValue(String.valueOf(replicationFactor))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("PARTITIONS")
                                        .withValue(String.valueOf(partitions))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TOPICS_COUNT")
                                        .withValue(String.valueOf(topicCount))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC_OFFSET")
                                        .withValue(String.valueOf(topicOffset))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("DEBUG")
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

    public static class Builder {
        protected int partitions;
        protected int replicationFactor;
        protected String topicOperation;
        private String adminName;
        private String bootstrapAddress;
        private String topicName;
        private int topicCount;
        private int topicOffset;
        private String additionalConfig;
        private String namespaceName;

        public KafkaAdminClient.Builder withAdminName(String adminName) {
            this.adminName = adminName;
            return this;
        }

        public KafkaAdminClient.Builder withBootstrapAddress(String bootstrapAddress) {
            this.bootstrapAddress = bootstrapAddress;
            return this;
        }

        public KafkaAdminClient.Builder withTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public KafkaAdminClient.Builder withTopicCount(int topicCount) {
            this.topicCount = topicCount;
            return this;
        }

        public KafkaAdminClient.Builder withTopicOffset(int topicOffset) {
            this.topicOffset = topicOffset;
            return this;
        }

        public KafkaAdminClient.Builder withAdditionalConfig(String additionalConfig) {
            this.additionalConfig = additionalConfig;
            return this;
        }

        public KafkaAdminClient.Builder withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public KafkaAdminClient.Builder withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public KafkaAdminClient.Builder withTopicOperation(String topicOperation) {
            this.topicOperation = topicOperation;
            return this;
        }

        public KafkaAdminClient.Builder withReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public KafkaAdminClient build() {
            return new KafkaAdminClient(this);
        }
    }
}
