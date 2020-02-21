/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly.cruisecontrol;

public enum CruiseControlParameters {

    DRY_RUN("dryrun"),
    JSON("json"),
    GOALS("goals"),
    VERBOSE("verbose"),
    FETCH_COMPLETE("fetch_completed_task"),
    USER_TASK_IDS("user_task_ids");

    String key;

    CruiseControlParameters(String key) {
        this.key = key;
    }

    public String asPair(String value) {
        return key + "=" + value;
    }

    public String asList(Iterable<String> values) {
        return key + "=" + String.join(",", values);
    }

}
