/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.HashMap;
import java.util.Map;

/**
 *  Holds the string literals for the possible options of the "Status" field in the JSON returned by the Cruise Control
 *  REST API /kafkacruisecontrol/user_tasks endpoint.
 *
 *  These values are taken directly from the Cruise Control source code for the TaskState enum at:
 *  com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager.TaskState
 *  https://github.com/linkedin/cruise-control/blob/master/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/servlet/UserTaskManager.java
 */
public enum CruiseControlUserTaskStatus {

    ACTIVE("Active"),
    IN_EXECUTION("InExecution"),
    COMPLETED("Completed"),
    COMPLETED_WITH_ERROR("CompletedWithError");

    private String status;
    // Reverse Lookup table
    private static final Map<String, CruiseControlUserTaskStatus> LOOKUP = new HashMap<>();
    //Populate the lookup table on loading time
    static {
        for (CruiseControlUserTaskStatus status : CruiseControlUserTaskStatus.values()) {
            LOOKUP.put(status.toString(), status);
        }
    }

    CruiseControlUserTaskStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return status;
    }

    public static CruiseControlUserTaskStatus lookup(String url) {
        return LOOKUP.get(url);
    }
}
