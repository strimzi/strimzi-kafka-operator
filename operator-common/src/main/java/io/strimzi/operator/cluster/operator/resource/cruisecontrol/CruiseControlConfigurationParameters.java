/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

public enum CruiseControlConfigurationParameters {

    CONCURRENT_PARTITION_MOVEMENTS("num.concurrent.partition.movements.per.broker"),
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("num.concurrent.intra.broker.partition.movements"),
    CONCURRENT_LEADER_MOVEMENTS("num.concurrent.leader.movements"),
    REPLICATION_THROTTLE("default.replication.throttle"),
    BROKER_METRICS_WINDOWS("num.broker.metrics.windows"),
    BROKER_METRICS_WINDOW_MS("broker.metrics.window.ms"),
    PARTITION_METRICS_WINDOWS("num.partition.metrics.windows"),
    PARTITION_METRICS_WINDOW_MS("partition.metrics.window.ms"),
    COMPLETED_USER_TASK_RETENTION_MS("completed.user.task.retention.time.ms"),
    CRUISE_CONTROL_PARTITION_METRICS_WINDOW_MS_CONFIG_KEY("partition.metrics.window.ms"),
    CRUISE_CONTROL_PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY("num.partition.metrics.windows"),
    CRUISE_CONTROL_BROKER_METRICS_WINDOW_MS_CONFIG_KEY("broker.metrics.window.ms"),
    CRUISE_CONTROL_BROKER_METRICS_WINDOW_NUM_CONFIG_KEY("num.broker.metrics.windows"),
    CRUISE_CONTROL_COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY("completed.user.task.retention.time.ms"),

    // Goals String lists
    CRUISE_CONTROL_GOALS_CONFIG_KEY("goals"),
    CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY("default.goals"),
    CRUISE_CONTROL_HARD_GOALS_CONFIG_KEY("hard.goals"),
    CRUISE_CONTROL_SELF_HEALING_CONFIG_KEY("self.healing.goals"),
    CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY("anomaly.detection.goals"),

    RACK_AWARENESS_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal"),
    REPLICA_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal"),
    DISK_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal"),
    NETWORK_INBOUND_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal"),
    NETWORK_OUTBOUND_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal"),
    CPU_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal"),
    REPLICA_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal"),
    POTENTIAL_NETWORK_OUTAGE_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal"),
    DISK_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal"),
    NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal"),
    NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal"),
    CPU_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal"),
    TOPIC_REPLICA_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal"),
    LEADER_REPLICA_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal"),
    LEADER_BYTES_IN_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal"),
    PREFERRED_LEADER_ELECTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal");

    private final String name;

    CruiseControlConfigurationParameters(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
