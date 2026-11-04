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
            "* `RebalanceOnScaleUp` if an auto-rebalance related to a scale-up operation is running.\n" +
            "* `RebalanceOnImbalance` if an auto-rebalance triggered by Cruise Control goal violation detection is running.")
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

    @Description("List of auto-rebalancing mode entries where an operation is currently running or queued. " +
            "For `add-brokers` or `remove-brokers` entries, the IDs of the brokers being rebalanced or waiting to be rebalanced are included. " +
            "For the `imbalance` entry, no broker IDs are included since the rebalance applies to the whole cluster.")
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
