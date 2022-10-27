/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener.arraylistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum KafkaListenerType {
    INTERNAL,
    ROUTE,
    LOADBALANCER,
    NODEPORT,
    INGRESS,
    CLUSTER_IP;

    @JsonCreator
    public static KafkaListenerType forValue(String value) {
        switch (value) {
            case "internal":
                return INTERNAL;
            case "route":
                return ROUTE;
            case "loadbalancer":
                return LOADBALANCER;
            case "nodeport":
                return NODEPORT;
            case "ingress":
                return INGRESS;
            case "cluster-ip":
                return CLUSTER_IP;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case INTERNAL:
                return "internal";
            case ROUTE:
                return "route";
            case LOADBALANCER:
                return "loadbalancer";
            case NODEPORT:
                return "nodeport";
            case INGRESS:
                return "ingress";
            case CLUSTER_IP:
                return "cluster-ip";
            default:
                return null;
        }
    }
}
