/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;


import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;

import static io.strimzi.operator.cluster.model.cruisecontrol.ResourceCapacityType.INBOUND_NETWORK;

/**
 * Cruise Control inbound network capacity configuration for broker.
 */
public class InboundNetworkCapacity extends NetworkCapacity {
    /**
     * Key used to identify resource in broker entry in Cruise Control capacity configuration.
     */
    public static final String KEY = "NW_IN";

    /**
     * Constructor
     *
     * Given the configured brokerCapacity, broker-specific capacity override, returns the capacity for the resource.
     *
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     */
    public InboundNetworkCapacity(BrokerCapacity brokerCapacity, BrokerCapacityOverride brokerCapacityOverride) {
        super(getThroughputInKiB(INBOUND_NETWORK.processResourceCapacity(brokerCapacity, brokerCapacityOverride, null)));
    }
}
