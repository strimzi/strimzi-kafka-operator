/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the status of an auto-rebalancing triggered by a cluster scaling request
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "state", "lastTransitionTime", "modes" })
@EqualsAndHashCode
@ToString
public class KafkaAutoRebalanceStatus implements UnknownPropertyPreserving {

    private KafkaAutoRebalanceState state;
    private String lastTransitionTime;
    private List<KafkaAutoRebalanceStatusBrokers> modes;
    private Map<String, Object> additionalProperties;

    @Description("The current state of an auto-rebalancing operation. Possible values are: \n\n" +
            "* `Idle` as the initial state when an auto-rebalancing is requested or as final state when it completes or fails.\n" +
            "* `RebalanceOnScaleDown` if an auto-rebalance related to a scale-down operation is running.\n" +
            "* `RebalanceOnScaleUp` if an auto-rebalance related to a scale-up operation is running.")
    public KafkaAutoRebalanceState getState() {
        return state;
    }

    public void setState(KafkaAutoRebalanceState state) {
        this.state = state;
    }

    @Description("The timestamp of the latest auto-rebalancing state update")
    public String getLastTransitionTime() {
        return lastTransitionTime;
    }

    public void setLastTransitionTime(String lastTransitionTime) {
        this.lastTransitionTime = lastTransitionTime;
    }

    @Description("List of modes where an auto-rebalancing operation is either running or queued. \n" +
            "Each mode entry (`add-brokers` or `remove-brokers`) includes one of the following: \n\n" +
            "* Broker IDs for a current auto-rebalance. \n" +
            "* Broker IDs for a queued auto-rebalance (if a previous rebalance is still in progress).")
    public List<KafkaAutoRebalanceStatusBrokers> getModes() {
        return modes;
    }

    public void setModes(List<KafkaAutoRebalanceStatusBrokers> modes) {
        this.modes = modes;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
