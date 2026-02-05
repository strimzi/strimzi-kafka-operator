/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.certmanager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum IssuerKind {
    ISSUER,
    CLUSTER_ISSUER;

    @JsonCreator
    public static IssuerKind forValue(String value) {
        switch (value) {
            case "Issuer":
                return ISSUER;
            case "ClusterIssuer":
                return CLUSTER_ISSUER;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case ISSUER:
                return "Issuer";
            case CLUSTER_ISSUER:
                return "ClusterIssuer";
            default:
                return null;
        }
    }
}
