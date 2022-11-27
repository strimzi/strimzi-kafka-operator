/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Enum with Cruise Control parameters
 */
public enum CruiseControlParameters {
    /**
     * Dry run
     */
    DRY_RUN("dryrun"),

    /**
     * JSON
     */
    JSON("json"),

    /**
     * Goals
     */
    GOALS("goals"),

    /**
     * Verbose
     */
    VERBOSE("verbose"),

    /**
     * Skip hard goals check
     */
    SKIP_HARD_GOAL_CHECK("skip_hard_goal_check"),

    /**
     * Reblance disk
     */
    REBALANCE_DISK("rebalance_disk"),

    /**
     * Fetch completed tasks
     */
    FETCH_COMPLETE("fetch_completed_task"),

    /**
     * User task IDs
     */
    USER_TASK_IDS("user_task_ids"),

    /**
     * Excluded topics
     */
    EXCLUDED_TOPICS("excluded_topics"),

    /**
     * Concurrent partition movements
     */
    CONCURRENT_PARTITION_MOVEMENTS("concurrent_partition_movements_per_broker"),

    /**
     * Concurrent intra-broker movements
     */
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("concurrent_intra_broker_partition_movements"),

    /**
     * Concurrent leader movements
     */
    CONCURRENT_LEADER_MOVEMENTS("concurrent_leader_movements"),

    /**
     * Replication throttle
     */
    REPLICATION_THROTTLE("replication_throttle"),

    /**
     * Replica movement strategies
     */
    REPLICA_MOVEMENT_STRATEGIES("replica_movement_strategies"),

    /**
     * Broker ID
     */
    BROKER_ID("brokerid");

    String key;

    /**
     * Creates the Enum from String
     *
     * @param key  String with the key
     */
    CruiseControlParameters(String key) {
        this.key = key;
    }

    /**
     * Returns the key-value pair
     *
     * @param value Value of the parameter
     *
     * @return  Key-value pair
     *
     * @throws UnsupportedEncodingException Thrown when UTF_8 encoding is not supported
     */
    public String asPair(String value) throws UnsupportedEncodingException {
        return key + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
    }

    /**
     * Returns the key-value pair with list value
     *
     * @param values Value of the parameter
     *
     * @return  Key-value pair
     *
     * @throws UnsupportedEncodingException Thrown when UTF_8 encoding is not supported
     */
    public String asList(Iterable<String> values) throws UnsupportedEncodingException {
        return key + "=" + URLEncoder.encode(String.join(",", values), StandardCharsets.UTF_8.toString());
    }

    @Override
    public String toString() {
        return key;
    }

}
