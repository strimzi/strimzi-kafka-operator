/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

public class RebalanceOptions {

    /** Sets whether this rebalance only provides an optimisation proposal (true) or starts a rebalance (false) */
    private boolean isDryRun;
    /** List of optimisation goal class names, must be a sub-set of the configured master goals and include all hard.goals unless skipHardGoals=true */
    private List<String> goals;
    /** Include additional information in the response from the Cruise Control Server */
    private boolean verbose;
    /** Allows specifying a custom goals list that does not incude all configured hard.goals */
    private boolean skipHardGoalCheck;
    /** Sets whether the response should be JSON formatted or formated for readibility on the command line */
    private boolean json = true;
    /** A regular expression to specify topics that should not be considered for replica movement */
    private String excludedTopics;
    /** The upper bound of ongoing replica movements going into/out of each broker */
    private int concurrentPartitionMovementsPerBroker;
    /** The upper bound of ongoing replica movements between disks within each broker */
    private int concurrentIntraBrokerPartitionMovements;
    /** The upper bound of ongoing leadership movements */
    private int concurrentLeaderMovements;
    /** The upper bound, in bytes per second, on the bandwidth used to move replicas */
    private long replicationThrottle;
    /** A list of strategy class names used to determine the execution order for the replica movements in the generated optimization proposal. */
    private List<String> replicaMovementStrategies;

    public boolean isDryRun() {
        return isDryRun;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isSkipHardGoalCheck() {
        return skipHardGoalCheck;
    }

    public List<String> getGoals() {
        return goals;
    }

    public boolean isJson() {
        return json;
    }

    public String getExcludedTopics() {
        return excludedTopics;
    }

    public int getConcurrentPartitionMovementsPerBroker() {
        return concurrentPartitionMovementsPerBroker;
    }

    public int getConcurrentIntraBrokerPartitionMovements() {
        return concurrentIntraBrokerPartitionMovements;
    }

    public int getConcurrentLeaderMovements() {
        return concurrentLeaderMovements;
    }

    public long getReplicationThrottle() {
        return replicationThrottle;
    }

    public List<String> getReplicaMovementStrategies() {
        return replicaMovementStrategies;
    }

    private RebalanceOptions(RebalanceOptionsBuilder builder) {
        this.isDryRun = builder.isDryRun;
        this.verbose = builder.verbose;
        this.skipHardGoalCheck = builder.skipHardGoalCheck;
        this.goals = builder.goals;
        this.verbose = builder.verbose;
        this.excludedTopics = builder.excludedTopics;
        this.concurrentPartitionMovementsPerBroker = builder.concurrentPartitionMovementsPerBroker;
        this.concurrentIntraBrokerPartitionMovements = builder.concurrentIntraPartitionMovements;
        this.concurrentLeaderMovements = builder.concurrentLeaderMovements;
        this.replicationThrottle = builder.replicationThrottle;
        this.replicaMovementStrategies = builder.replicaMovementStrategies;
    }

    public static class RebalanceOptionsBuilder {

        private boolean isDryRun;
        private boolean verbose;
        private boolean skipHardGoalCheck;
        private List<String> goals;
        private String excludedTopics;
        private int concurrentPartitionMovementsPerBroker;
        private int concurrentIntraPartitionMovements;
        private int concurrentLeaderMovements;
        private long replicationThrottle;
        private List<String> replicaMovementStrategies;

        public RebalanceOptionsBuilder() {
            isDryRun = true;
            verbose = false;
            skipHardGoalCheck = false;
            goals = null;
            excludedTopics = null;
            concurrentPartitionMovementsPerBroker = 0;
            concurrentIntraPartitionMovements = 0;
            concurrentLeaderMovements = 0;
            replicationThrottle = 0;
            replicaMovementStrategies = null;
        }

        public RebalanceOptionsBuilder withFullRun() {
            this.isDryRun = false;
            return this;
        }

        public RebalanceOptionsBuilder withVerboseResponse() {
            this.verbose = true;
            return this;
        }

        public RebalanceOptionsBuilder withSkipHardGoalCheck() {
            this.skipHardGoalCheck = true;
            return this;
        }

        public RebalanceOptionsBuilder withGoals(List<String> goals) {
            this.goals = goals;
            return this;
        }

        public RebalanceOptionsBuilder withExcludedTopics(String excludedTopics) {
            this.excludedTopics = excludedTopics;
            return this;
        }

        public RebalanceOptionsBuilder withConcurrentPartitionMovementsPerBroker(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent movements between brokers should be greater than zero");
            }
            this.concurrentPartitionMovementsPerBroker = movements;
            return this;
        }

        public RebalanceOptionsBuilder withConcurrentIntraPartitionMovements(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent intra partition movements should be greater than zero");
            }
            this.concurrentIntraPartitionMovements = movements;
            return this;
        }

        public RebalanceOptionsBuilder withConcurrentLeaderMovements(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent partition leadership movements should be greater than zero");
            }
            this.concurrentLeaderMovements = movements;
            return this;
        }

        public RebalanceOptionsBuilder withReplicationThrottle(long bandwidth) {
            if (bandwidth < 0) {
                throw new IllegalArgumentException("The max replication bandwidth should be greater than zero");
            }
            this.replicationThrottle = bandwidth;
            return this;
        }

        public RebalanceOptionsBuilder withReplicaMovementStrategies(List<String> replicaMovementStrategies) {
            this.replicaMovementStrategies = replicaMovementStrategies;
            return this;
        }

        public RebalanceOptions build() {
            return new RebalanceOptions(this);
        }



    }

}
