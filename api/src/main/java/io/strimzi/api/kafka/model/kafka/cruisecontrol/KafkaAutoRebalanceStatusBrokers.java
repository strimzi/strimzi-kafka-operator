/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a mode related to an auto-rebalancing operation in progress or queued,
 * together with the list of brokers' IDs involved in the rebalancing itself
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"mode", "brokers"})
@EqualsAndHashCode
@ToString
public class KafkaAutoRebalanceStatusBrokers implements UnknownPropertyPreserving {

    private KafkaRebalanceMode mode;
    private List<Integer> brokers;
    private Map<String, Object> additionalProperties;

    @Description("Mode for which there is an auto-rebalancing operation in progress or queued, when brokers are added or removed. " +
            "The possible modes are `add-brokers` and `remove-brokers`.")
    public KafkaRebalanceMode getMode() {
        return mode;
    }

    public void setMode(KafkaRebalanceMode mode) {
        this.mode = mode;
    }

    @Description("List of brokers' IDs involved in an auto-rebalancing operation related to current mode. " +
            "it contains either: " +
            "- the brokers' IDs relevant to the current ongoing auto-rebalance, or " +
            "- the brokers' IDs relevant to a queued auto-rebalance (if a previous auto-rebalance is still in progress)")
    public List<Integer> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<Integer> brokers) {
        this.brokers = brokers;
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
