/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public enum CruiseControlParameters {

    DRY_RUN("dryrun"),
    JSON("json"),
    GOALS("goals"),
    VERBOSE("verbose"),
    SKIP_HARD_GOAL_CHECK("skip_hard_goal_check"),
    FETCH_COMPLETE("fetch_completed_task"),
    USER_TASK_IDS("user_task_ids"),
    EXCLUDED_TOPICS("excluded_topics"),
    CONCURRENT_PARTITION_MOVEMENTS("concurrent_partition_movements_per_broker"),
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("concurrent_intra_broker_partition_movements"),
    CONCURRENT_LEADER_MOVEMENTS("concurrent_leader_movements"),
    REPLICATION_THROTTLE("replication_throttle"),
    REPLICA_MOVEMENT_STRATEGIES("replica_movement_strategies");

    String key;

    CruiseControlParameters(String key) {
        this.key = key;
    }

    public String asPair(String value) throws UnsupportedEncodingException {
        return key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
    }

    public String asList(Iterable<String> values) throws UnsupportedEncodingException {
        return key + "=" + URLEncoder.encode(String.join(",", values), StandardCharsets.UTF_8.toString());
    }

    public String toString() {
        return key;
    }

}
