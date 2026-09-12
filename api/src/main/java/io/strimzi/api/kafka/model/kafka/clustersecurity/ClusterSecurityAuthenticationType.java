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
    MTLS,
    SERVICE_ACCOUNT;

    @JsonCreator
    public static ClusterSecurityAuthenticationType forValue(String value) {
        return switch (value) {
            case "none" -> NONE;
            case "mtls", "strimzi-mtls" -> MTLS; // We have to keep the legacy strimzi-mtls here for downgrades/upgrades to/from 1.2.0
            case "service-account" -> SERVICE_ACCOUNT;
            default -> throw new IllegalArgumentException("Unknown authentication type: " + value);
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case NONE -> "none";
            case MTLS -> "strimzi-mtls"; // We have to keep the legacy strimzi-mtls here for downgrades/upgrades to/from 1.2.0
            case SERVICE_ACCOUNT -> "service-account";
        };
    }
}
