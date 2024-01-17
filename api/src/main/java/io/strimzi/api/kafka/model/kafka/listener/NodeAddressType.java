/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum NodeAddressType {
    EXTERNAL_IP,
    EXTERNAL_DNS,
    INTERNAL_IP,
    INTERNAL_DNS,
    HOSTNAME;

    @JsonCreator
    public static NodeAddressType forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "externalip":
                return EXTERNAL_IP;
            case "externaldns":
                return EXTERNAL_DNS;
            case "internalip":
                return INTERNAL_IP;
            case "internaldns":
                return INTERNAL_DNS;
            case "hostname":
                return HOSTNAME;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case EXTERNAL_IP:
                return "ExternalIP";
            case EXTERNAL_DNS:
                return "ExternalDNS";
            case INTERNAL_IP:
                return "InternalIP";
            case INTERNAL_DNS:
                return "InternalDNS";
            case HOSTNAME:
                return "Hostname";
            default:
                return null;
        }
    }
}
