/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

@SuppressWarnings("unchecked")
public abstract class AbstractRebalanceOptions {

    /** Sets whether this rebalance only provides an optimisation proposal (true) or starts a rebalance (false) */
    private boolean isDryRun;
    /** List of optimisation goal class names, must be a sub-set of the configured main goals and include all hard.goals unless skipHardGoals=true */
    private List<String> goals;
    /** Include additional information in the response from the Cruise Control Server */
    private boolean verbose;
    /** Allows specifying a custom goals list that does not incude all configured hard.goals */
    private boolean skipHardGoalCheck;
    /** Sets whether the response should be JSON formatted or formated for readibility on the command line */
    private boolean json;
    /** A regular expression to specify topics that should not be considered for replica movement */
    private String excludedTopics;
    /** The upper bound of ongoing replica movements going into/out of each broker */
    private int concurrentPartitionMovementsPerBroker;
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

    public int getConcurrentLeaderMovements() {
        return concurrentLeaderMovements;
    }

    public long getReplicationThrottle() {
        return replicationThrottle;
    }

    public List<String> getReplicaMovementStrategies() {
        return replicaMovementStrategies;
    }

    AbstractRebalanceOptions(AbstractRebalanceOptionsBuilder builder) {
        this.isDryRun = builder.isDryRun;
        this.goals = builder.goals;
        this.verbose = builder.verbose;
        this.skipHardGoalCheck = builder.skipHardGoalCheck;
        this.json = builder.json;
        this.excludedTopics = builder.excludedTopics;
        this.concurrentPartitionMovementsPerBroker = builder.concurrentPartitionMovementsPerBroker;
        this.concurrentLeaderMovements = builder.concurrentLeaderMovements;
        this.replicationThrottle = builder.replicationThrottle;
        this.replicaMovementStrategies = builder.replicaMovementStrategies;
    }

    public abstract static class AbstractRebalanceOptionsBuilder<B extends AbstractRebalanceOptionsBuilder<B, T>, T extends AbstractRebalanceOptions> {

        private boolean isDryRun;
        private List<String> goals;
        private boolean verbose;
        private boolean skipHardGoalCheck;
        private boolean json;
        private String excludedTopics;
        private int concurrentPartitionMovementsPerBroker;
        private int concurrentLeaderMovements;
        private long replicationThrottle;
        private List<String> replicaMovementStrategies;

        AbstractRebalanceOptionsBuilder() {
            isDryRun = true;
            goals = null;
            verbose = false;
            skipHardGoalCheck = false;
            json = true;
            excludedTopics = null;
            concurrentPartitionMovementsPerBroker = 0;
            concurrentLeaderMovements = 0;
            replicationThrottle = 0;
            replicaMovementStrategies = null;
        }

        protected abstract B self();

        public B withFullRun() {
            this.isDryRun = false;
            return self();
        }

        public B withVerboseResponse() {
            this.verbose = true;
            return self();
        }

        public B withSkipHardGoalCheck() {
            this.skipHardGoalCheck = true;
            return self();
        }

        public B withGoals(List<String> goals) {
            this.goals = goals;
            return self();
        }

        public B withExcludedTopics(String excludedTopics) {
            this.excludedTopics = excludedTopics;
            return self();
        }

        public B withConcurrentPartitionMovementsPerBroker(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent movements between brokers should be greater than zero");
            }
            this.concurrentPartitionMovementsPerBroker = movements;
            return self();
        }

        public B withConcurrentLeaderMovements(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent partition leadership movements should be greater than zero");
            }
            this.concurrentLeaderMovements = movements;
            return self();
        }

        public B withReplicationThrottle(long bandwidth) {
            if (bandwidth < 0) {
                throw new IllegalArgumentException("The max replication bandwidth should be greater than zero");
            }
            this.replicationThrottle = bandwidth;
            return self();
        }

        public B withReplicaMovementStrategies(List<String> replicaMovementStrategies) {
            this.replicaMovementStrategies = replicaMovementStrategies;
            return self();
        }
    }
}
