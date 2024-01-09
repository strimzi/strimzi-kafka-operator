/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.List;

/**
 * Rebalance options for removing disks from the Kafka cluster
 */
public class RemoveDisksOptions extends AbstractRebalanceOptions {
    /** list with the ids of the brokers removed from the cluster */
    private final List<String> brokersandlogDirs;

    /**
     * @return  List of brokers which will be removed
     */
    public List<String> getBrokersandLogDirs() {
        return brokersandlogDirs;
    }

    private RemoveDisksOptions(RemoveDisksOptionsBuilder builder) {
        super(builder);
        this.brokersandlogDirs = builder.brokersandLogDirs;
    }

    /**
     * Builder class for RemoveBrokerOptions
     */
    public static class RemoveDisksOptionsBuilder extends AbstractRebalanceOptionsBuilder<RemoveDisksOptionsBuilder, RemoveDisksOptions> {
        private List<String> brokersandLogDirs;

        /**
         * Constructor
         */
        public RemoveDisksOptionsBuilder() {
            this.brokersandLogDirs = null;
        }

        @Override
        protected RemoveDisksOptionsBuilder self() {
            return this;
        }

        /**
         * List of brokers which should be removed
         *
         * @param brokersandLogDirs   List of broker IDs
         *
         * @return  Instance of this builder
         */
        public RemoveDisksOptionsBuilder withBrokersandLogDirs(List<String> brokersandLogDirs) {
            this.brokersandLogDirs = brokersandLogDirs;
            return this;
        }

        @Override
        public RemoveDisksOptions build() {
            return new RemoveDisksOptions(this);
        }
    }
}
