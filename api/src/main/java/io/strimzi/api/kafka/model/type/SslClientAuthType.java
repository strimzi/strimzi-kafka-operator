/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SslClientAuthType {
    NONE,
    REQUIRED,
    REQUESTED;

    @JsonCreator
    public static SslClientAuthType forValue(String value) {
        switch (value) {
            case "none":
                return NONE;
            case "requested":
                return REQUESTED;
            case "required":
                return REQUIRED;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case NONE:
                return "none";
            case REQUESTED:
                return "requested";
            case REQUIRED:
                return "required";
            default:
                return null;
        }
    }
}
