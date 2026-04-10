/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum KafkaListenerType {
    INTERNAL,
    ROUTE,
    TLSROUTE,
    LOADBALANCER,
    NODEPORT,
    INGRESS,
    CLUSTER_IP;

    @JsonCreator
    public static KafkaListenerType forValue(String value) {
        return switch (value) {
            case "internal" -> INTERNAL;
            case "route" -> ROUTE;
            case "tlsroute" -> TLSROUTE;
            case "loadbalancer" -> LOADBALANCER;
            case "nodeport" -> NODEPORT;
            case "ingress" -> INGRESS;
            case "cluster-ip" -> CLUSTER_IP;
            default -> null;
        };
    }

    @JsonValue
    public String toValue() {
        return switch (this) {
            case INTERNAL -> "internal";
            case ROUTE -> "route";
            case TLSROUTE -> "tlsroute";
            case LOADBALANCER -> "loadbalancer";
            case NODEPORT -> "nodeport";
            case INGRESS -> "ingress";
            case CLUSTER_IP -> "cluster-ip";
        };
    }
}
