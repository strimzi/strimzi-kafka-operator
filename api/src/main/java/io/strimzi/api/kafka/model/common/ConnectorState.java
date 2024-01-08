/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * This defines values for the spec.state field of connectors
 */
public enum ConnectorState {
    PAUSED,
    STOPPED,
    RUNNING;

    @JsonCreator
    public static ConnectorState forValue(String value) {
        switch (value) {
            case "paused":
                return PAUSED;
            case "stopped":
                return STOPPED;
            case "running":
                return RUNNING;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case PAUSED:
                return "paused";
            case STOPPED:
                return "stopped";
            case RUNNING:
                return "running";
            default:
                return null;
        }
    }
}
