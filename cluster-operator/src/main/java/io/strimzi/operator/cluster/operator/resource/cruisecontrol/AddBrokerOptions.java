/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

public class AddBrokerOptions extends AbstractRebalanceOptions {

    /** list with the ids of the new brokers added to the cluster */
    private List<Integer> brokers;

    public List<Integer> getBrokers() {
        return brokers;
    }

    private AddBrokerOptions(AddBrokerOptionsBuilder builder) {
        super(builder);
        this.brokers = builder.brokers;
    }

    public static class AddBrokerOptionsBuilder extends AbstractRebalanceOptions.AbstractRebalanceOptionsBuilder<AddBrokerOptionsBuilder, AddBrokerOptions> {

        private List<Integer> brokers;

        public AddBrokerOptionsBuilder() {
            this.brokers = null;
        }

        @Override
        protected AddBrokerOptionsBuilder self() {
            return this;
        }

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
