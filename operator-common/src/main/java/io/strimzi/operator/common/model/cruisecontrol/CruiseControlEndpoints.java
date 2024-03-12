/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * Enum with Cruise Control API endpoints
 */
public enum CruiseControlEndpoints {
    /**
     * State
     */
    STATE("/kafkacruisecontrol/state"),

    /**
     * Rebalance
     */
    REBALANCE("/kafkacruisecontrol/rebalance"),

    /**
     * Stop execution
     */
    STOP("/kafkacruisecontrol/stop_proposal_execution"),

    /**
     * User tasks
     */
    USER_TASKS("/kafkacruisecontrol/user_tasks"),

    /**
     * Add broker
     */
    ADD_BROKER("/kafkacruisecontrol/add_broker"),

    /**
     * Remove broker
     */
    REMOVE_BROKER("/kafkacruisecontrol/remove_broker"),

    /**
     * Topic configuration
     */
    TOPIC_CONFIGURATION("/kafkacruisecontrol/topic_configuration");

    private final String path;

    /**
     * Creates the Enum from String
     *
     * @param path  String with the path
     */
    CruiseControlEndpoints(String path) {
        this.path = path;
    }

    /**
     * @return  Value as a String
     */
    public String toString() {
        return path;
    }
}
