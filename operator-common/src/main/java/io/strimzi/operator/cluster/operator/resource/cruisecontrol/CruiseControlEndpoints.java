/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

public enum CruiseControlEndpoints {

    STATE("/kafkacruisecontrol/state"),
    REBALANCE("/kafkacruisecontrol/rebalance"),
    STOP("/kafkacruisecontrol/stop_proposal_execution"),
    USER_TASKS("/kafkacruisecontrol/user_tasks");

    String path;

    CruiseControlEndpoints(String path) {
        this.path = path;
    }

    public String toString() {
        return path;
    }


}
