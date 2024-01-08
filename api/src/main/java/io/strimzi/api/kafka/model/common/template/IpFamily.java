/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum IpFamily {
    IPV4,
    IPV6;

    @JsonCreator
    public static IpFamily forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "ipv4":
                return IPV4;
            case "ipv6":
                return IPV6;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case IPV4:
                return "IPv4";
            case IPV6:
                return "IPv6";
            default:
                return null;
        }
    }
}
