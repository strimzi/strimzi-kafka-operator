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
        switch (value) {
            case "strimzi.io":
                return STRIMZI_IO;
            case "cert-manager.io":
                return CERT_MANAGER_IO;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case STRIMZI_IO:
                return "strimzi.io";
            case CERT_MANAGER_IO:
                return "cert-manager.io";
            default:
                return null;
        }
    }
}
