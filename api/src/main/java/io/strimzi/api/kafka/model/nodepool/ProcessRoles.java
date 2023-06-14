/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/**
 * KRaft process roles which Kafka nodes can have: controller and broker
 */
public enum ProcessRoles {
    CONTROLLER,
    BROKER;

    @JsonCreator
    public static ProcessRoles forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "controller":
                return CONTROLLER;
            case "broker":
                return BROKER;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case CONTROLLER:
                return "controller";
            case BROKER:
                return "broker";
            default:
                return null;
        }
    }
}
