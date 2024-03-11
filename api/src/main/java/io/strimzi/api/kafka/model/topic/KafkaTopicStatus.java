/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.topic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a status of the KafkaTopic resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "topicName", "topicId", "replicasChange" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaTopicStatus extends Status {
    private static final long serialVersionUID = 1L;

    private String topicName;
    private String topicId;
    private ReplicasChangeStatus replicasChange;

    @Description("Topic name")
    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @Description("The topic's id. For a KafkaTopic with the ready condition, " +
        "this will change only if the topic gets deleted and recreated with the same name.")
    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Replication factor change status.")
    public ReplicasChangeStatus getReplicasChange() {
        return replicasChange;
    }

    public void setReplicasChange(ReplicasChangeStatus replicasChange) {
        this.replicasChange = replicasChange;
    }
}
