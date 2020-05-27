/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

public class CruiseControlUserTaskResponse extends CruiseControlResponse {

    private boolean completedWithError;

    CruiseControlUserTaskResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        this.completedWithError = false;
    }

    public boolean completedWithError() {
        return completedWithError;
    }

    public void setCompletedWithError(boolean completedWithError) {
        this.completedWithError = completedWithError;
    }


}
