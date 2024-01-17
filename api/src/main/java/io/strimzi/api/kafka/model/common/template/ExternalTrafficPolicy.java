/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum ExternalTrafficPolicy {
    LOCAL,
    CLUSTER;

    @JsonCreator
    public static ExternalTrafficPolicy forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "local":
                return LOCAL;
            case "cluster":
                return CLUSTER;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case CLUSTER:
                return "Cluster";
            case LOCAL:
                return "Local";
            default:
                return null;
        }
    }
}
