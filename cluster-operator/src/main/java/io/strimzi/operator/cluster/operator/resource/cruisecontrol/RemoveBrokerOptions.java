/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

public class RemoveBrokerOptions extends AbstractRebalanceOptions {

    /** list with the ids of the brokers removed from the cluster */
    private List<String> brokers;

    public List<String> getBrokers() {
        return brokers;
    }

    private RemoveBrokerOptions(RemoveBrokerOptionsBuilder builder) {
        super(builder);
        this.brokers = builder.brokers;
    }

    public static class RemoveBrokerOptionsBuilder extends AbstractRebalanceOptionsBuilder<RemoveBrokerOptionsBuilder, RemoveBrokerOptions> {

        private List<String> brokers;

        public RemoveBrokerOptionsBuilder() {
            this.brokers = null;
        }

        @Override
        protected RemoveBrokerOptionsBuilder self() {
            return this;
        }

        public RemoveBrokerOptionsBuilder withBrokers(List<String> brokers) {
            this.brokers = brokers;
            return this;
        }

        public RemoveBrokerOptions build() {
            return new RemoveBrokerOptions(this);
        }
    }
}
