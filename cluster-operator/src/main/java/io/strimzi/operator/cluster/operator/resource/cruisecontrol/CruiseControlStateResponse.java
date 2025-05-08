/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Cruise Control response from `/kafkacruisecontrol/state` endpoint.
 */
public class CruiseControlStateResponse extends CruiseControlResponse {
    private static final String EXECUTOR_STATE_KEY = "ExecutorState";

    private final ExecutorStatus executorStatus;

    /**
     * Constructor
     *
     * @param userTaskId    User task ID
     * @param json          JSON data
     */
    CruiseControlStateResponse(String userTaskId, JsonNode json) {
        super(userTaskId, json);

        executorStatus = new ExecutorStatus(json.get(EXECUTOR_STATE_KEY));
    }

    /**
     * @return Executor status
     */
    public ExecutorStatus getExecutorStatus() {
        return this.executorStatus;
    }
}
