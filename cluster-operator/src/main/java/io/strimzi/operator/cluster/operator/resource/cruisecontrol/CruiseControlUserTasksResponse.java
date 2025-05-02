/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;

import java.time.Instant;

/**
 * Response to user tasks request
 */
public class CruiseControlUserTasksResponse extends CruiseControlResponse {

    private static final String STATUS_KEY = "Status";
    private static final String START_MS_KEY = "StartMs";

    private CruiseControlUserTaskStatus status;
    private Instant taskStartTime;
    private boolean isMaxActiveUserTasksReached;

    /**
     * Constructor
     *
     * @param userTaskId    User task ID
     * @param json          JSON data
     */
    CruiseControlUserTasksResponse(String userTaskId, JsonNode json) {
        super(userTaskId, json);
        // The maximum number of active user tasks that can run concurrently has reached
        // Sourced from the error message that contains "reached the servlet capacity" from the Cruise Control response
        this.isMaxActiveUserTasksReached = false;
        setTaskStatus(json);
        setTaskStartTime(json);
    }

    /**
     * @return  True If the maximum number of active user tasks that can run concurrently has reached
     */
    public boolean isMaxActiveUserTasksReached() {
        return isMaxActiveUserTasksReached;
    }

    protected void setMaxActiveUserTasksReached(boolean maxActiveUserTasksReached) {
        this.isMaxActiveUserTasksReached = maxActiveUserTasksReached;
    }

    private void setTaskStatus(JsonNode userTaskJson) {
        if (userTaskJson != null && userTaskJson.has(STATUS_KEY)) {
            this.status = CruiseControlUserTaskStatus.lookup(userTaskJson.get("Status").asText());
        }
    }

    /**
     * @return status of user task.
     */
    public CruiseControlUserTaskStatus getTaskStatus() {
        return this.status;
    }

    /**
     * Extracts the task start time from the provided `userTaskJson` JSON object.
     * The task start time is extracted from the "StartMs" field, which contains a
     * timestamp in of Unix epoch in milliseconds. The timestamp is then parsed into Instant object.
     *
     * @param userTaskJson The `JsonNode` object containing the user task json,
     *                      from which the task start time will be extracted.
     */
    private void setTaskStartTime(JsonNode userTaskJson) {
        if (userTaskJson != null && userTaskJson.has(START_MS_KEY)) {
            // Extract the task start time as Unix epoch in milliseconds
            long taskStartTimeInMilliseconds = userTaskJson.get(START_MS_KEY).asLong();

            this.taskStartTime = Instant.ofEpochMilli(taskStartTimeInMilliseconds);
        }
    }

    /**
     * @return Start time of user task as an Instant object
     */
    public Instant getTaskStartTime() {
        return this.taskStartTime;
    }

}
