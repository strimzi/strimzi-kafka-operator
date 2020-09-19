/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

public enum CruiseControlGoals {

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

    CruiseControlGoals(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}