/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * This enum contains the keys used in the various levels of the JSON object returned by the Cruise Control
 * rebalance endpoint.
 */
public enum CruiseControlRebalanceKeys {
    /**
     * Summary
     */
    SUMMARY("summary"),

    /**
     * Original response
     */
    ORIGINAL_RESPONSE("originalResponse"),

    /**
     * Load before optimization
     */
    LOAD_BEFORE_OPTIMIZATION("loadBeforeOptimization"),

    /**
     * Load after optimization
     */
    LOAD_AFTER_OPTIMIZATION("loadAfterOptimization"),

    /**
     * Brokers
     */
    BROKERS("brokers"),

    /**
     * Broker
     */
    BROKER_ID("Broker");

    private final String key;

    /**
     * Creates the Enum from String
     *
     * @param key  String with the key
     */
    CruiseControlRebalanceKeys(String key) {
        this.key = key;
    }

    /**
     * @return  The key as a String
     */
    public String getKey() {
        return key;
    }
}
