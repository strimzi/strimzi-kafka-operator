/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;


import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;

import static io.strimzi.operator.cluster.model.cruisecontrol.ResourceCapacityType.OUTBOUND_NETWORK;

/**
 * Cruise Control outbound network capacity configuration for broker.
 */
public class OutboundNetworkCapacity extends NetworkCapacity {
    /**
     * Key used to identify resource in broker entry in Cruise Control capacity configuration.
     */
    public static final String KEY = "NW_OUT";

    /**
     * Constructor
     *
     * Given the configured brokerCapacity, broker-specific capacity override, returns the capacity for the resource.
     *
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     */
    public OutboundNetworkCapacity(BrokerCapacity brokerCapacity, BrokerCapacityOverride brokerCapacityOverride) {
        super(getThroughputInKiB(OUTBOUND_NETWORK.processResourceCapacity(brokerCapacity, brokerCapacityOverride, null)));
    }
}
