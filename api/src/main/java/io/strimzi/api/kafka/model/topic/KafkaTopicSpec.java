/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.topic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"topicName", "partitions", "replicas", "config"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaTopicSpec extends Spec {

    private static final long serialVersionUID = 1L;

    private String topicName;

    private Integer partitions;

    private Integer replicas;

    private Map<String, Object> config;

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
            "especially for topics with semantic partitioning. " +
            "When absent this will default to the broker configuration for `num.partitions`.")
    @Minimum(1)
    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    @Description("The number of replicas the topic should have. " +
            "When absent this will default to the broker configuration for `default.replication.factor`.")
    @Minimum(1)
    @Maximum(Short.MAX_VALUE)
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
}
