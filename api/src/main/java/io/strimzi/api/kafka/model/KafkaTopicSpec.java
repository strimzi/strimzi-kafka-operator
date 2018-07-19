/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonPropertyOrder({"partitions", "replicas", "config"})
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
public class KafkaTopicSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topicName;

    private int partitions;

    private int replicas;

    private Map<String, Object> config;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The name of the topic. " +
            "When absent this will default to the metadata.name of the topic. " +
            "It is recommended to not set this unless the topic name is not a " +
            "valid Kubernetes resource name.")
    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitions() {
        return partitions;
    }

    @Description("The number of partitions the topic should have. " +
            "This cannot be decreased after topic creation. " +
            "It can be increased after topic creation, " +
            "but it is important to understand the consequences that has, " +
            "especially for topics with semantic partitioning.")
    @Minimum(1)
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    @Description("The number of replicas the topic should have.")
    @Minimum(1)
    @Maximum(Short.MAX_VALUE)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The topic configuration.")
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, Object> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }
}
