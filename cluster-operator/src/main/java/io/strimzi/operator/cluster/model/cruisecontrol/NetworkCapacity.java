/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;


import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;
import io.strimzi.operator.cluster.model.StorageUtils;

import static io.strimzi.operator.cluster.model.cruisecontrol.ResourceCapacityType.INBOUND_NETWORK;
import static io.strimzi.operator.cluster.model.cruisecontrol.ResourceCapacityType.OUTBOUND_NETWORK;

/**
 * Cruise Control network capacity configuration for broker.
 */
public class NetworkCapacity {
    protected static final String DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000KiB/s";

    private final String config;

    /**
     * Constructor
     *
     * Given the configured brokerCapacity, broker-specific capacity override, returns the capacity for the resource.
     *
     * @param type                   The resource capacity type. Must be network-related (e.g. INBOUND_NETWORK, OUTBOUND_NETWORK).
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     */
    public NetworkCapacity(ResourceCapacityType type, BrokerCapacity brokerCapacity, BrokerCapacityOverride brokerCapacityOverride) {
        if (type != INBOUND_NETWORK && type != OUTBOUND_NETWORK) {
            throw new IllegalArgumentException("Unsupported resource type for NetworkCapacity: " + type);
        }

        this.config = getThroughputInKiB(type.processResourceCapacity(brokerCapacity, brokerCapacityOverride, null));
    }

    /**
     * Parse Strimzi representation of throughput, such as {@code 10000KB/s},
     * into the equivalent number of kibibytes represented as a String.
     *
     * @param throughput The String representation of the throughput.
     * @return The equivalent number of kibibytes.
     */
    public static String getThroughputInKiB(String throughput) {
        String size = throughput.substring(0, throughput.indexOf("B"));
        return String.valueOf(StorageUtils.convertTo(size, "Ki"));
    }

    /**
     * Returns CpuCapacity object as a JsonObject
     *
     * @return The CpuCapacity object as a JsonObject
     */
    @Override
    public String toString() {
        return config;
    }
}
