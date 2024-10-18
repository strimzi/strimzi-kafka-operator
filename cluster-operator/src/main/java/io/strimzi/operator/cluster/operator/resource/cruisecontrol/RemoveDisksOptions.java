/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIds;

import java.util.List;

/**
 * Rebalance options for removing disks from brokers within the Kafka cluster
 */
public class RemoveDisksOptions extends AbstractRebalanceOptions {
    /** list with the ID's of the broker and volumes which will be used by remove-disks endpoint */
    private final List<BrokerAndVolumeIds> brokerAndVolumeIds;

    /**
     * @return  List of brokers and volume ID's which will be used by remove-disks endpoint
     */
    public List<BrokerAndVolumeIds> getBrokersandVolumeIds() {
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
        private List<BrokerAndVolumeIds> brokerAndVolumeIdsList;

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
         * List of broker and volume ID's to be used
         *
         * @param brokerAndVolumeIdsList   List of broker and volume ID's
         *
         * @return  Instance of this builder
         */
        public RemoveDisksOptionsBuilder withBrokersandVolumeIds(List<BrokerAndVolumeIds> brokerAndVolumeIdsList) {
            this.brokerAndVolumeIdsList = brokerAndVolumeIdsList;
            return this;
        }

        @Override
        public RemoveDisksOptions build() {
            return new RemoveDisksOptions(this);
        }
    }
}
