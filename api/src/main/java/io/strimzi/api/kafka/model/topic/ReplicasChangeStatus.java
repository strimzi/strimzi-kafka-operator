/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.topic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "targetReplicas", "state", "userTaskId", "message", "sessionId" })
@EqualsAndHashCode
@ToString(callSuper = true)
public class ReplicasChangeStatus implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Integer targetReplicas;
    private ReplicasChangeState state;
    private String sessionId;
    private String message;
    private Map<String, Object> additionalProperties;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The target replicas value requested by the user. " +
        "This may be different from .spec.replicas when a change is ongoing.")
    public Integer getTargetReplicas() {
        return targetReplicas;
    }

    public void setTargetReplicas(Integer targetReplicas) {
        this.targetReplicas = targetReplicas;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Current state of the replicas change operation. This can be `pending`, when the change has been " +
        "requested, or `ongoing`, when the change has been successfully submitted to Cruise Control.")
    public ReplicasChangeState getState() {
        return state;
    }

    public void setState(ReplicasChangeState state) {
        this.state = state;
    }
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The session identifier for replicas change requests pertaining to this KafkaTopic resource. " +
        "This is used by the Topic Operator to track the status of `ongoing` replicas change operations.")
    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Message for the user related to the replicas change request. " +
        "This may contain transient error messages that would disappear on periodic reconciliations.")
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
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
