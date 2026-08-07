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

        // ExecutorState is only present when not using substates parameter or when using certain substates
        // When querying with substates=anomaly_detector, only AnomalyDetectorState is present
        JsonNode executorStateNode = json.get(EXECUTOR_STATE_KEY);
        if (executorStateNode != null && !executorStateNode.isNull()) {
            executorStatus = new ExecutorStatus(executorStateNode);
        } else {
            executorStatus = null;
        }
    }

    /**
     * Gets the executor status from the Cruise Control response.
     *
     * @return Executor status
     */
    public ExecutorStatus getExecutorStatus() {
        return this.executorStatus;
    }
}
