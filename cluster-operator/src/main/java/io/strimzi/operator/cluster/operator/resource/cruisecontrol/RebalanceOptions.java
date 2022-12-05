/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

/**
 * Rebalance options for full Kafka cluster rebalance
 */
public class RebalanceOptions extends AbstractRebalanceOptions {
    /** Whether to balance load between disks within brokers (requires JBOD Kafka deployment) */
    private boolean rebalanceDisk;
    /** The upper bound of ongoing replica movements between disks within each broker */
    private int concurrentIntraBrokerPartitionMovements;

    /**
     * @return  True if intra-broker relbalance is enabled. False otherwise.
     */
    public boolean isRebalanceDisk() {
        return rebalanceDisk;
    }

    /**
     * @return  Number of concurrent intra-broker partition movements
     */
    public int getConcurrentIntraBrokerPartitionMovements() {
        return concurrentIntraBrokerPartitionMovements;
    }

    RebalanceOptions(RebalanceOptionsBuilder builder) {
        super(builder);
        this.rebalanceDisk = builder.rebalanceDisk;
        this.concurrentIntraBrokerPartitionMovements = builder.concurrentIntraPartitionMovements;
    }

    /**
     * Builder for the full rebalance options
     */
    public static class RebalanceOptionsBuilder extends AbstractRebalanceOptionsBuilder<RebalanceOptionsBuilder, RebalanceOptions> {
        private boolean rebalanceDisk;
        private int concurrentIntraPartitionMovements;

        /**
         * Constructor
         */
        public RebalanceOptionsBuilder() {
            rebalanceDisk = false;
            concurrentIntraPartitionMovements = 0;
        }

        @Override
        protected RebalanceOptionsBuilder self() {
            return this;
        }

        /**
         * Enabled intra-broker rebalancing
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withRebalanceDisk() {
            this.rebalanceDisk = true;
            return self();
        }

        /**
         * Number of concurrent intra-broker partition movements
         *
         * @param movements Number of intra-broker partition movements
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withConcurrentIntraPartitionMovements(int movements) {
            if (movements < 0) {
                throw new IllegalArgumentException("The max number of concurrent intra partition movements should be greater than zero");
            }
            this.concurrentIntraPartitionMovements = movements;
            return self();
        }

        @Override
        public RebalanceOptions build() {
            return new RebalanceOptions(this);
        }
    }

}
