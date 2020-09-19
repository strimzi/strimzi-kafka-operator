/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"partitions", "replicas", "config"})
@EqualsAndHashCode
public class KafkaTopicSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private String topicName;

    private Integer partitions;

    private Integer replicas;

    private Map<String, Object> config;

    private Map<String, Object> additionalProperties;

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

    @Description("The number of partitions the topic should have. " +
            "This cannot be decreased after topic creation. " +
            "It can be increased after topic creation, " +
            "but it is important to understand the consequences that has, " +
            "especially for topics with semantic partitioning.")
    @Minimum(1)
    @JsonProperty(required = true)
    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    @Description("The number of replicas the topic should have.")
    @Minimum(1)
    @Maximum(Short.MAX_VALUE)
    @JsonProperty(required = true)
    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    @Description("The topic configuration.")
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
