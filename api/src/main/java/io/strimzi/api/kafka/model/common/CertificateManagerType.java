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
    STRIMZI_IO,
    CERT_MANAGER_IO;

    @JsonCreator
    public static CertificateManagerType forValue(String value) {
        return switch (value) {
            case "strimzi.io" -> STRIMZI_IO;
            case "cert-manager.io" -> CERT_MANAGER_IO;
            default -> throw new IllegalArgumentException(String.format("Unknown certificate manager type: %s. Must be %s or %s.", value, STRIMZI_IO.toValue(), CERT_MANAGER_IO.toValue()));
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case STRIMZI_IO -> "strimzi.io";
            case CERT_MANAGER_IO -> "cert-manager.io";
        };
    }
}
