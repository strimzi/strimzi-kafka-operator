/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

/**
 * Abstract class for different sets of rebalance options - e.g. regular rebalance, add-brokers rebalance, remove-brokers rebalance.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractRebalanceOptions {
    /** Sets whether this rebalance only provides an optimisation proposal (true) or starts a rebalance (false) */
    private final boolean isDryRun;
    /** List of optimisation goal class names, must be a sub-set of the configured main goals and include all hard.goals unless skipHardGoals=true */
    private final List<String> goals;
    /** Include additional information in the response from the Cruise Control Server */
    private final boolean verbose;
    /** Allows specifying a custom goals list that does not include all configured hard.goals */
    private final boolean skipHardGoalCheck;
    /** Sets whether the response should be JSON formatted or formatted for readability on the command line */
    private final boolean json;
    /** A regular expression to specify topics that should not be considered for replica movement */
    private final String excludedTopics;
    /** The upper bound of ongoing replica movements going into/out of each broker */
    private final int concurrentPartitionMovementsPerBroker;
    /** The upper bound of ongoing leadership movements */
    private final int concurrentLeaderMovements;
    /** The upper bound, in bytes per second, on the bandwidth used to move replicas */
    private final long replicationThrottle;
    /** A list of strategy class names used to determine the execution order for the replica movements in the generated optimization proposal. */
    private final List<String> replicaMovementStrategies;

    /**
     * @return  True if this is dry-run only. False otherwise.
     */
    public boolean isDryRun() {
        return isDryRun;
    }

    /**
     * @return  True if the rebalance should use verbose response. False otherwise.
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * @return  True if hard-goals check should be skipped. False otherwise.
     */
    public boolean isSkipHardGoalCheck() {
        return skipHardGoalCheck;
    }

    /**
     * @return  List of goals
     */
    public List<String> getGoals() {
        return goals;
    }

    /**
     * @return  True if response should be in the JSON format. False otherwise.
     */
    public boolean isJson() {
        return json;
    }

    /**
     * @return  Excludes topics
     */
    public String getExcludedTopics() {
        return excludedTopics;
    }

    /**
     * @return  Number of concurrent partition movements per broker
     */
    public int getConcurrentPartitionMovementsPerBroker() {
        return concurrentPartitionMovementsPerBroker;
    }

    /**
     * @return  Number of concurrent movements
     */
    public int getConcurrentLeaderMovements() {
        return concurrentLeaderMovements;
    }

    /**
     * @return  Replication throttle
     */
    public long getReplicationThrottle() {
        return replicationThrottle;
    }

    /**
     * @return  List of configured replica movement strategies
     */
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

    /**
     * Abstract class for rebalance options builder
     *
     * @param <B>   Rebalance Options Builder type
     * @param <T>   Rebalance Options type
     */
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

        /**
         * @return  Builds the rebalance options and returns them
         */
        public abstract T build();

        /**
         * Enable full run
         *
         * @return  Instance of this builder
         */
        public B withFullRun() {
            this.isDryRun = false;
            return self();
        }

        /**
         * Enable verbose response
         *
         * @return  Instance of this builder
         */
        public B withVerboseResponse() {
            this.verbose = true;
            return self();
        }

        /**
         * Skip hard goals check
         *
         * @return  Instance of this builder
         */
        public B withSkipHardGoalCheck() {
            this.skipHardGoalCheck = true;
            return self();
        }

        /**
         * Set rebalance goals
         *
         * @param goals     List of rebalance goals
         *
         * @return  Instance of this builder
         */
        public B withGoals(List<String> goals) {
            this.goals = goals;
            return self();
        }

        /**
         * Set excluded topics
         *
         * @param excludedTopics    Excluded topics
         *
         * @return  Instance of this builder
         */
        public B withExcludedTopics(String excludedTopics) {
            this.excludedTopics = excludedTopics;
            return self();
        }

        /**
         * Sets number of concurrent partition movements per-broker
         *
         * @param movements Number of movements
         *
         * @return  Instance of this builder
         */
        public B withConcurrentPartitionMovementsPerBroker(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent movements between brokers should be greater than zero");
            }
            this.concurrentPartitionMovementsPerBroker = movements;
            return self();
        }

        /**
         * Sets number of concurrent leader movements
         *
         * @param movements     Number of leader movements
         *
         * @return  Instance of this builder
         */
        public B withConcurrentLeaderMovements(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent partition leadership movements should be greater than zero");
            }
            this.concurrentLeaderMovements = movements;
            return self();
        }

        /**
         * Sets replication throttle
         *
         * @param bandwidth     The throttle bandwidth
         *
         * @return  Instance of this builder
         */
        public B withReplicationThrottle(long bandwidth) {
            if (bandwidth < 0) {
                throw new IllegalArgumentException("The max replication bandwidth should be greater than zero");
            }
            this.replicationThrottle = bandwidth;
            return self();
        }

        /**
         * Sets replica movement strategies
         *
         * @param replicaMovementStrategies     List of replica movement strategies
         *
         * @return  Instance of this builder
         */
        public B withReplicaMovementStrategies(List<String> replicaMovementStrategies) {
            this.replicaMovementStrategies = replicaMovementStrategies;
            return self();
        }
    }
}
