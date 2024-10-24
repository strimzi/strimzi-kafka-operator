/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum defining the supported mode for a manual rebalancing
 * by using the KafkaRebalance custom resource
 */
public enum KafkaRebalanceMode {
    FULL("full"),
    ADD_BROKERS("add-brokers"),
    REMOVE_BROKERS("remove-brokers"),
    REMOVE_DISKS("remove-disks");

    private final String name;

    KafkaRebalanceMode(String name) {
        this.name = name;
    }

    @JsonCreator
    public static KafkaRebalanceMode forValue(String value) {
        switch (value) {
            case "full":
                return FULL;
            case "add-brokers":
                return ADD_BROKERS;
            case "remove-brokers":
                return REMOVE_BROKERS;
            case "remove-disks":
                return REMOVE_DISKS;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        return this.name;
    }
}
