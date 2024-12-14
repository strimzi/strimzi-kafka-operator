/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DnsPolicy {
    CLUSTERFIRST,
    CLUSTERFIRSTWITHHOSTNET,
    DEFAULT,
    NONE;

    @JsonCreator
    public static DnsPolicy forValue(String value) {
        switch (value) {
            case "ClusterFirst":
                return CLUSTERFIRST;
            case "ClusterFirstWithHostNet":
                return CLUSTERFIRSTWITHHOSTNET;
            case "Default":
                return DEFAULT;
            case "None":
                return NONE;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case CLUSTERFIRST:
                return "ClusterFirst";
            case CLUSTERFIRSTWITHHOSTNET:
                return "ClusterFirstWithHostNet";
            case DEFAULT:
                return "Default";
            case NONE:
                return "None";
            default:
                return null;
        }
    }
}
