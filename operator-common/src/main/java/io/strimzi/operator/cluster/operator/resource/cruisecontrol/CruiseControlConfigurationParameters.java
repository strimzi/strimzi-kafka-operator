/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.concurrent.TimeUnit;

public enum CruiseControlConfigurationParameters {

    // 'goals' and 'default.goals' are currently not defined, due to unnecessary dependency pulling for now
    CONCURRENT_PARTITION_MOVEMENTS("num.concurrent.partition.movements.per.broker", 5),
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("num.concurrent.intra.broker.partition.movements", 2),
    CONCURRENT_LEADER_MOVEMENTS("num.concurrent.leader.movements", 1000),
    REPLICATION_THROTTLE("default.replication.throttle", -1),
    BROKER_METRICS_WINDOWS("num.broker.metrics.windows", "20"),
    BROKER_METRICS_WINDOW_MS("broker.metrics.window.ms", Integer.toString(300_000)),
    PARTITION_METRICS_WINDOWS("num.partition.metrics.windows", "1"),
    PARTITION_METRICS_WINDOW_MS("partition.metrics.window.ms", Integer.toString(300_000)),
    COMPLETED_USER_TASK_RETENTION_MS("completed.user.task.retention.time.ms", Long.toString(TimeUnit.DAYS.toMillis(1))),
    CRUISE_CONTROL_PARTITION_METRICS_WINDOW_MS_CONFIG_KEY("partition.metrics.window.ms", Integer.toString(300_000)),
    CRUISE_CONTROL_PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY("num.partition.metrics.windows", "1"),
    CRUISE_CONTROL_BROKER_METRICS_WINDOW_MS_CONFIG_KEY("broker.metrics.window.ms", Integer.toString(300_000)),
    CRUISE_CONTROL_BROKER_METRICS_WINDOW_NUM_CONFIG_KEY("num.broker.metrics.windows", "20"),
    CRUISE_CONTROL_COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY("completed.user.task.retention.time.ms", Long.toString(TimeUnit.DAYS.toMillis(1)));

    private final String name;
    private final Object defaultValue;

    CruiseControlConfigurationParameters(String name, Object defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return name;
    }
}
