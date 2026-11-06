/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.RebalanceNode;

import java.util.List;

/**
 * Rebalance options for removing disks from brokers within the Kafka cluster
 */
public class RemoveDisksOptions extends AbstractRebalanceOptions {
    /** list with the IDs of the broker and volumes which will be used by remove-disks endpoint */
    private final List<RebalanceNode> brokerAndVolumeIds;

    /**
     * Gets the list of brokers and volume IDs for the remove-disks endpoint.
     *
     * @return  List of brokers and volume IDs which will be used by remove-disks endpoint
     */
    public List<RebalanceNode> getBrokersandVolumeIds() {
        return brokerAndVolumeIds;
    }

    private RemoveDisksOptions(RemoveDisksOptionsBuilder builder) {
        super(builder);
        this.brokerAndVolumeIds = builder.brokerAndVolumeIdsList;
    }

    /**
     * Builder class for RemoveDisksOptions
     */
    public static class RemoveDisksOptionsBuilder extends AbstractRebalanceOptionsBuilder<RemoveDisksOptionsBuilder, RemoveDisksOptions> {
        private List<RebalanceNode> brokerAndVolumeIdsList;

        /**
         * Constructor
         */
        public RemoveDisksOptionsBuilder() {
            this.brokerAndVolumeIdsList = null;
        }

        @Override
        protected RemoveDisksOptionsBuilder self() {
            return this;
        }

        /**
         * List of broker and volume IDs to be used
         *
         * @param brokerAndVolumeIdsList   List of broker and volume IDs
         *
         * @return  Instance of this builder
         */
        public RemoveDisksOptionsBuilder withBrokersandVolumeIds(List<RebalanceNode> brokerAndVolumeIdsList) {
            this.brokerAndVolumeIdsList = brokerAndVolumeIdsList;
            return this;
        }

        @Override
        public RemoveDisksOptions build() {
            return new RemoveDisksOptions(this);
        }
    }
}
