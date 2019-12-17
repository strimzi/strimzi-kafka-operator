/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a status of the Kafka Cluster Rebalance resource
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "sessionId", "optimizationResult" })
@EqualsAndHashCode
@ToString(callSuper = true)
public class KafkaClusterRebalanceStatus extends Status {

    private static final long serialVersionUID = 1L;

    private String sessionId;
    private Map<String, Object> optimizationResult = new HashMap<>(0);

    @Description("A JSON describing the optimization result")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getOptimizationResult() {
        return optimizationResult;
    }

    public void setOptimizationResult(Map<String, Object> optimizationResult) {
        this.optimizationResult = optimizationResult;
    }

    @Description("The session identifier for requests to Cruise Control pertaining to this KafkaClusterRebalance resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
