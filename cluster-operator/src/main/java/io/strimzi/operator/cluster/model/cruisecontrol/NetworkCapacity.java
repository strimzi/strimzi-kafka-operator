/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.operator.cluster.model.StorageUtils;

/**
 * Cruise Control network capacity configuration for broker.
 */
public class NetworkCapacity {
    protected static final String DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000KiB/s";
    protected String config;

    protected NetworkCapacity(String config) {
        this.config = config;
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

    @Override
    public String toString() {
        return config;
    }
}
