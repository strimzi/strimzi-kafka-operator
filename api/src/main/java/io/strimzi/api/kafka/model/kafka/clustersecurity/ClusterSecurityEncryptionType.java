/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.clustersecurity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents supported encryption types for ClusterSecurity
 */
public enum ClusterSecurityEncryptionType {
    NONE,
    STRIMZI_TLS;

    @JsonCreator
    public static ClusterSecurityEncryptionType forValue(String value) {
        return switch (value) {
            case "none" -> NONE;
            case "strimzi-tls" -> STRIMZI_TLS;
            default -> null;
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case NONE -> "none";
            case STRIMZI_TLS -> "strimzi-tls";
        };
    }
}
