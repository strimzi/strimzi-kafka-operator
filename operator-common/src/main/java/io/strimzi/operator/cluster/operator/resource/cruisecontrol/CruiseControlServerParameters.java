package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.concurrent.TimeUnit;

public enum CruiseControlServerParameters {

    CONCURRENT_PARTITION_MOVEMENTS("num.concurrent.partition.movements.per.broker", 5),
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("num.concurrent.intra.broker.partition.movements", 2),
    CONCURRENT_LEADER_MOVEMENTS("num.concurrent.leader.movements", 1000),
    REPLICATION_THROTTLE("default.replication.throttle", -1),
    BROKER_METRICS_WINDOWS("num.broker.metrics.windows", "20"),
    BROKER_METRICS_WINDOW_MS("broker.metrics.window.ms", Integer.toString(300_000)),
    PARTITION_METRICS_WINDOWS("num.partition.metrics.windows", "1"),
    PARTITION_METRICS_WINDOW_MS("partition.metrics.window.ms", Integer.toString(300_000)),
    COMPLETED_USER_TASK_RETENTION_MS("completed.user.task.retention.time.ms", Long.toString(TimeUnit.DAYS.toMillis(1))),
    // TODO io/strimzi/operator/cluster/model/CruiseControlConfiguration.java:101
    GOALS("goals", xxx),
    DEFAULT_GOALS("default.goals", XXX);

    private final String name;
    private final Object defaultValue;

    CruiseControlServerParameters(String name, Object defaulValue) {
            this.name = name;
            this.defaultValue = defaulValue;
    }

    public String getName() {
        return name;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }
}
