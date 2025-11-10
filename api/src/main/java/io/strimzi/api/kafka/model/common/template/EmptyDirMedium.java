/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum EmptyDirMedium {
    MEMORY;

    @JsonCreator
    public static EmptyDirMedium forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "memory":
                return MEMORY;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case MEMORY:
                return "Memory";
            default:
                return null;
        }
    }
}
