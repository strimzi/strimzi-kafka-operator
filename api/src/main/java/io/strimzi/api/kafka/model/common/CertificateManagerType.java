/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Defines values for the spec.clusterCa/clientsCa.type field
 */
public enum CertificateManagerType {
    STRIMZI,
    CERT_MANAGER;

    @JsonCreator
    public static CertificateManagerType forValue(String value) {
        return switch (value) {
            case "strimzi" -> STRIMZI;
            case "cert-manager" -> CERT_MANAGER;
            default -> throw new IllegalArgumentException(String.format("Unknown certificate manager type: %s. Must be %s or %s.", value, STRIMZI.toValue(), CERT_MANAGER.toValue()));
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case STRIMZI -> "strimzi";
            case CERT_MANAGER -> "cert-manager";
        };
    }
}
