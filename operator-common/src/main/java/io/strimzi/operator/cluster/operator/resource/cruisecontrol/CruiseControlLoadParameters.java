/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This enum contains keys for the broker before and after load JSON object. Each entry contains the Cruise Control key,
 * taken from the com.linkedin.kafka.cruisecontrol.servlet.response.stats.BasicStats class, and a more readable key name
 * used for the KafkaRebalance Status. The type field is used to distinguish key that require different castings when
 * extracted to create the [before, after, difference] arrays for the KafkaRebalance status.
 */
public enum CruiseControlLoadParameters {

    LEADERS("Leaders", "leaders", "int"),
    REPLICAS("Replicas", "replicas", "int"),
    CPU_PERCENTAGE("CpuPct", "cpuPercentage", "double"),
    DISK_PERCENTAGE("DiskPct", "diskUsedPercentage", "double"),
    DISK_MB("DiskMB", "diskUsedMB", "double"),
    NETWORK_OUT_RATE("NwOutRate", "networkOutRate", "double"),
    LEADER_NETWORK_IN_RATE("LeaderNwInRate", "leaderNetworkInRate", "double"),
    FOLLOWER_NETWORK_IN_RATE("FollowerNwInRate", "followerNetworkInRate", "double"),
    POTENTIAL_MAX_NETWORK_OUT_RATE("PnwOutRate", "potentialMaxNetworkOutRate", "double");

    /** The key used in the load JSON object returned by Cruise Control. */
    private String cruiseControlKey;
    /** The key used for the KafakRebalance status field. */
    private String strimziKey;
    /** The type of value stored in the relevant field. */
    private String type;

    CruiseControlLoadParameters(String cruiseControlKey, String strimziKey, String type) {
        this.cruiseControlKey = cruiseControlKey;
        this.strimziKey = strimziKey;
        this.type = type;
    }

    public String getCruiseControlKey() {
        return cruiseControlKey;
    }

    public String getStrimziKey() {
        return strimziKey;
    }

    public String getType() {
        return type;
    }

    private static List<CruiseControlLoadParameters> filterByType(String filterType) {
        return Arrays.stream(CruiseControlLoadParameters.values())
                .filter(loadParameter -> loadParameter.getType() != null)
                .filter(loadParameter -> loadParameter.getType().equals(filterType))
                .collect(Collectors.toList());
    }

    public static List<CruiseControlLoadParameters> getIntegerParameters() {
        return filterByType("int");
    }

    public static List<CruiseControlLoadParameters> getDoubleParameters() {
        return filterByType("double");
    }

}
