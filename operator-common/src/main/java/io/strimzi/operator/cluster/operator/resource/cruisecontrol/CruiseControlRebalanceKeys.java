/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

/**
 * This enum contains the keys used in the various levels of the JSON object returned by the Cruise Control
 * rebalance endpoint.
 */
public enum CruiseControlRebalanceKeys {

    SUMMARY("summary"),
    ORIGINAL_RESPONSE("originalResponse"),
    LOAD_BEFORE_OPTIMIZATION("loadBeforeOptimization"),
    LOAD_AFTER_OPTIMIZATION("loadAfterOptimization"),
    BROKERS("brokers"),
    BROKER_ID("Broker");

    private String key;

    CruiseControlRebalanceKeys(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}
