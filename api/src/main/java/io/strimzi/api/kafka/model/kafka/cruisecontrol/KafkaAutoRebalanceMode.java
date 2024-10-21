/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum defining the supported mode for a autorebalancing
 */
public enum KafkaAutoRebalanceMode {
    ADD_BROKERS("add-brokers"),
    REMOVE_BROKERS("remove-brokers");

    private final String name;

    KafkaAutoRebalanceMode(String name) {
        this.name = name;
    }

    @JsonCreator
    public static KafkaAutoRebalanceMode forValue(String value) {
        switch (value) {
            case "add-brokers":
                return ADD_BROKERS;
            case "remove-brokers":
                return REMOVE_BROKERS;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        return this.name;
    }
}
