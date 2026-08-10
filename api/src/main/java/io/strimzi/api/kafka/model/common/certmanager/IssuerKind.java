/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.certmanager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The type of Issuer to use for Certificate resources that will be reconciled by cert-manager.
 * cert-manager supports two kinds: Issuer and ClusterIssuer.
 */
public enum IssuerKind {
    /**
     * Issuer kind that is referenced by Certificate resources in the same namespace
     */
    ISSUER,
    /**
     * Issuer that kind that can be referenced by Certificate resources in any namespace
     */
    CLUSTER_ISSUER;

    @JsonCreator
    public static IssuerKind forValue(String value) {
        switch (value) {
            case "Issuer":
                return ISSUER;
            case "ClusterIssuer":
                return CLUSTER_ISSUER;
            default:
                throw new IllegalArgumentException("Unknown IssuerKind: " + value + ". Must be 'Issuer' or 'ClusterIssuer'.");
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
