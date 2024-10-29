/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

/**
 * Response to user tasks request
 */
public class CruiseControlUserTasksResponse extends CruiseControlResponse {
    private boolean isMaxActiveUserTasksReached;

    /**
     * Constructor
     *
     * @param userTaskId    User task ID
     * @param json          JSON data
     */
    CruiseControlUserTasksResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        // The maximum number of active user tasks that can run concurrently has reached
        // Sourced from the error message that contains "reached the servlet capacity" from the Cruise Control response
        this.isMaxActiveUserTasksReached = false;
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
}
