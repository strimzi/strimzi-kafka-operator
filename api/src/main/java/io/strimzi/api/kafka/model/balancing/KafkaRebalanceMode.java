/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.balancing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum KafkaRebalanceMode {
    FULL_REBALANCE,
    ADD_BROKER,
    REMOVE_BROKER;

    @JsonCreator
    public static KafkaRebalanceMode forValue(String value) {
        switch (value) {
            case "full-rebalance":
                return FULL_REBALANCE;
            case "add-broker":
                return ADD_BROKER;
            case "remove-broker":
                return REMOVE_BROKER;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case FULL_REBALANCE:
                return "full-rebalance";
            case ADD_BROKER:
                return "add-broker";
            case REMOVE_BROKER:
                return "remove-broker";
            default:
                return null;
        }
    }
}
