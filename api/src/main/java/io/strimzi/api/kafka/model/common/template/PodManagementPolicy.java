/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum PodManagementPolicy {
    ORDERED_READY,
    PARALLEL;

    @JsonCreator
    public static PodManagementPolicy forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "orderedready":
                return ORDERED_READY;
            case "parallel":
                return PARALLEL;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case PARALLEL:
                return "Parallel";
            case ORDERED_READY:
                return "OrderedReady";
            default:
                return null;
        }
    }
}
