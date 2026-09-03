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
    TLS;

    @JsonCreator
    public static ClusterSecurityEncryptionType forValue(String value) {
        return switch (value) {
            case "none" -> NONE;
            case "tls", "strimzi-tls" -> TLS; // We have to keep the legacy strimzi-tls here for downgrades/upgrades to/from 1.2.0
            default -> null;
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case NONE -> "none";
            case TLS -> "strimzi-tls"; // We have to keep the legacy strimzi-tls here for downgrades/upgrades to/from 1.2.0
        };
    }
}
