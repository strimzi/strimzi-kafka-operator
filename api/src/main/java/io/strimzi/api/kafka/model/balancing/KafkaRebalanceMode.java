/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.balancing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum KafkaRebalanceMode {
    FULL_REBALANCE("full-rebalance"),
    ADD_BROKER("add-broker"),
    REMOVE_BROKER("remove-broker");

    private String name;

    KafkaRebalanceMode(String name) {
        this.name = name;
    }

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
        return this.name;
    }
}
