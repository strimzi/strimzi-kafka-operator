/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum DeploymentStrategy {
    ROLLING_UPDATE,
    RECREATE;

    @JsonCreator
    public static DeploymentStrategy forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "rollingupdate":
                return ROLLING_UPDATE;
            case "recreate":
                return RECREATE;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case RECREATE:
                return "Recreate";
            case ROLLING_UPDATE:
                return "RollingUpdate";
            default:
                return null;
        }
    }
}
