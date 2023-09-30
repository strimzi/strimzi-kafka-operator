/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

/**
 * Rebalance options for removing brokers from the Kafka cluster
 */
public class RemoveBrokerOptions extends AbstractRebalanceOptions {
    /** list with the ids of the brokers removed from the cluster */
    private final List<Integer> brokers;

    /**
     * @return  List of brokers which will be removed
     */
    public List<Integer> getBrokers() {
        return brokers;
    }

    private RemoveBrokerOptions(RemoveBrokerOptionsBuilder builder) {
        super(builder);
        this.brokers = builder.brokers;
    }

    /**
     * Builder class for RemoveBrokerOptions
     */
    public static class RemoveBrokerOptionsBuilder extends AbstractRebalanceOptionsBuilder<RemoveBrokerOptionsBuilder, RemoveBrokerOptions> {
        private List<Integer> brokers;

        /**
         * Constructor
         */
        public RemoveBrokerOptionsBuilder() {
            this.brokers = null;
        }

        @Override
        protected RemoveBrokerOptionsBuilder self() {
            return this;
        }

        /**
         * List of brokers which should be removed
         *
         * @param brokers   List of broker IDs
         *
         * @return  Instance of this builder
         */
        public RemoveBrokerOptionsBuilder withBrokers(List<Integer> brokers) {
            this.brokers = brokers;
            return this;
        }

        @Override
        public RemoveBrokerOptions build() {
            return new RemoveBrokerOptions(this);
        }
    }
}
