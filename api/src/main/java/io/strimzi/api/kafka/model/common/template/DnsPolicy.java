/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DnsPolicy {
    CLUSTER_FIRST,
    CLUSTER_FIRST_WITH_HOST_NET,
    DEFAULT,
    NONE;

    @JsonCreator
    public static DnsPolicy forValue(String value) {
        switch (value) {
            case "ClusterFirst":
                return CLUSTER_FIRST;
            case "ClusterFirstWithHostNet":
                return CLUSTER_FIRST_WITH_HOST_NET;
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
            case CLUSTER_FIRST:
                return "ClusterFirst";
            case CLUSTER_FIRST_WITH_HOST_NET:
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
