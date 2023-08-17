/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * Enum with Cruise Control goals
 */
public enum CruiseControlGoals {
    /**
     * Rack aware goal
     */
    RACK_AWARENESS_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal"),

    /**
     * Minimum topic leaders per broker goal
     */
    MIN_TOPIC_LEADERS_PER_BROKER_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal"),

    /**
     * Replica capacity goal
     */
    REPLICA_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal"),

    /**
     * Disk capacity goal
     */
    DISK_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal"),

    /**
     * Inbound network goal
     */
    NETWORK_INBOUND_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal"),

    /**
     * outbound network goal
     */
    NETWORK_OUTBOUND_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal"),

    /**
     * CPU capacity goal
     */
    CPU_CAPACITY_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal"),

    /**
     * Replica distribution goal
     */
    REPLICA_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal"),

    /**
     * Potential network outage goal
     */
    POTENTIAL_NETWORK_OUTAGE_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal"),

    /**
     * Disk usage distribution goal
     */
    DISK_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal"),

    /**
     * Inbound network distribution goal
     */
    NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal"),

    /**
     * Outbound network distribution goal
     */
    NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal"),

    /**
     * CPU usage distribution goal
     */
    CPU_USAGE_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal"),

    /**
     * Topic replica distribution goal
     */
    TOPIC_REPLICA_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal"),

    /**
     * Leader replica distribution goal
     */
    LEADER_REPLICA_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal"),

    /**
     * Leader bytes-in replica distribution goal
     */
    LEADER_BYTES_IN_DISTRIBUTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal"),

    /**
     * Preferred Leader election goal
     */
    PREFERRED_LEADER_ELECTION_GOAL("com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal");

    private final String name;

    /**
     * Creates the Enum from String
     *
     * @param name  String with the path
     */
    CruiseControlGoals(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}