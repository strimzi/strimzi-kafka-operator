/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

/**
 * Rebalance options for adding a new broker(s) to the cluster
 */
public class AddBrokerOptions extends AbstractRebalanceOptions {
    /** list with the ids of the new brokers added to the cluster */
    private List<Integer> brokers;

    /**
     * @return  List of broker IDs which should be added
     */
    public List<Integer> getBrokers() {
        return brokers;
    }

    private AddBrokerOptions(AddBrokerOptionsBuilder builder) {
        super(builder);
        this.brokers = builder.brokers;
    }

    /**
     * The builder class for building AddBrokerOptions
     */
    public static class AddBrokerOptionsBuilder extends AbstractRebalanceOptions.AbstractRebalanceOptionsBuilder<AddBrokerOptionsBuilder, AddBrokerOptions> {
        private List<Integer> brokers;

        /**
         * Constructs the builder
         */
        public AddBrokerOptionsBuilder() {
            this.brokers = null;
        }

        @Override
        protected AddBrokerOptionsBuilder self() {
            return this;
        }

        /**
         * Add list of brokers which should be added
         *
         * @param brokers   List of broker IDs
         *
         * @return  Instance of this builder
         */
        public AddBrokerOptionsBuilder withBrokers(List<Integer> brokers) {
            this.brokers = brokers;
            return this;
        }

        @Override
        public AddBrokerOptions build() {
            return new AddBrokerOptions(this);
        }
    }
}
