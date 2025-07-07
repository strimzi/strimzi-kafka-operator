/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;

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
        super(getThroughputInKiB(processResourceCapacity(brokerCapacity, brokerCapacityOverride)));
    }

    /**
     * Given the configured brokerCapacity, broker-specific capacity override, and broker resource requirements,
     * returns the capacity for the resource.
     *
     * <p>
     * The broker-specific capacity override takes top precedence, then general brokerCapacity configuration,
     * then resource default.
     *
     * For example:
     * <ul>
     *   <li> (1) The brokerCapacityOverride for a specific broker.
     *   <li> (2) The general brokerCapacity configuration.
     *   <li> (3) The resource default.
     * </ul>
     *
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     *
     * @return The capacity of resource represented as a String.
     */
    private static String processResourceCapacity(BrokerCapacity brokerCapacity,
                                          BrokerCapacityOverride brokerCapacityOverride) {
        if (brokerCapacityOverride != null && brokerCapacityOverride.getInboundNetwork() != null) {
            return brokerCapacityOverride.getInboundNetwork();
        }

        if (brokerCapacity != null && brokerCapacity.getInboundNetwork() != null) {
            return brokerCapacity.getInboundNetwork();
        }

        return DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
    }
}
