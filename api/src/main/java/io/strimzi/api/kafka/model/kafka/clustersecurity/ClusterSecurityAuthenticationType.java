/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.clustersecurity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents supported authentication types for ClusterSecurity
 */
public enum ClusterSecurityAuthenticationType {
    NONE,
    STRIMZI_MTLS;

    @JsonCreator
    public static ClusterSecurityAuthenticationType forValue(String value) {
        return switch (value) {
            case "none" -> NONE;
            case "strimzi-mtls" -> STRIMZI_MTLS;
            default -> null;
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case NONE -> "none";
            case STRIMZI_MTLS -> "strimzi-mtls";
        };
    }
}
