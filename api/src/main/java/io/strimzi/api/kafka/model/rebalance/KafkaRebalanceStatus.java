/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a status of the Kafka Rebalance resource
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "sessionId", "optimizationResult" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaRebalanceStatus extends Status {

    private static final long serialVersionUID = 1L;

    private String sessionId;
    private Map<String, Object> optimizationResult = new HashMap<>(0);

    @Description("A JSON object describing the optimization result")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getOptimizationResult() {
        return optimizationResult;
    }

    public void setOptimizationResult(Map<String, Object> optimizationResult) {
        this.optimizationResult = optimizationResult;
    }

    @Description("The session identifier for requests to Cruise Control pertaining to this KafkaRebalance resource. " +
            "This is used by the Kafka Rebalance operator to track the status of ongoing rebalancing operations.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
