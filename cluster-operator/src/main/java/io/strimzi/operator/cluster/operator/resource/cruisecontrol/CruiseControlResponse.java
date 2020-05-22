/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

public class CruiseControlResponse {

    private String userTaskId;
    private JsonObject json;

    CruiseControlResponse(String userTaskId, JsonObject json) {
        this.userTaskId = userTaskId;
        this.json = json;
    }

    public String getUserTaskId() {
        return userTaskId;
    }

    public JsonObject getJson() {
        return json;
    }

    public String prettyPrint() {
        return "User Task ID: " + userTaskId + "\nJSON:\n " + json.encodePrettily();
    }

    public String toString() {
        return "User Task ID: " + userTaskId + " JSON: " + json.toString();
    }


}
