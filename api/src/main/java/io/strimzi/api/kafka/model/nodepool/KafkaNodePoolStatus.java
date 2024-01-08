/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.crdgenerator.annotations.AddedIn;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Represents a status of the KafkaNodePool resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "nodeIds", "clusterId", "roles", "replicas", "labelSelector" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaNodePoolStatus extends Status {
    private static final long serialVersionUID = 1L;

    private List<Integer> nodeIds;
    private String clusterId;
    private List<ProcessRoles> roles;

    // Replicas and label selector are required for scale subresource
    private int replicas;
    private String labelSelector;

    @Description("Node IDs used by Kafka nodes in this pool")
    public List<Integer> getNodeIds() {
        return nodeIds;
    }

    public void setNodeIds(List<Integer> nodeIds) {
        this.nodeIds = nodeIds;
    }

    @Description("Kafka cluster ID")
    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @AddedIn("0.39.0")
    @Description("The roles currently assigned to this pool.")
    public List<ProcessRoles> getRoles() {
        return roles;
    }

    public void setRoles(List<ProcessRoles> roles) {
        this.roles = roles;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The current number of pods being used to provide this resource.")
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Label selector for pods providing this resource.")
    public String getLabelSelector() {
        return labelSelector;
    }

    public void setLabelSelector(String labelSelector) {
        this.labelSelector = labelSelector;
    }
}
