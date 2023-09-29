/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This enum contains keys for the JSON object that contains the broker before and after load information. Each entry
 * contains the Cruise Control keys, taken from the com.linkedin.kafka.cruisecontrol.servlet.response.stats.BasicStats
 * class, and a more readable key name used for the KafkaRebalance status. The type field is used to distinguish key
 * that require different castings when extracted to create the [before, after, difference] arrays for the
 * KafkaRebalance status.
 */
public enum CruiseControlLoadParameters {
    /**
     * Leaders parameter
     */
    LEADERS("Leaders", "leaders", "int"),

    /**
     * Replicas parameter
     */
    REPLICAS("Replicas", "replicas", "int"),

    /**
     * CPU percentage parameter
     */
    CPU_PERCENTAGE("CpuPct", "cpuPercentage", "double"),

    /**
     * Disk percentage parameter
     */
    DISK_PERCENTAGE("DiskPct", "diskUsedPercentage", "double"),

    /**
     * Disk size parameter
     */
    DISK_MB("DiskMB", "diskUsedMB", "double"),

    /**
     * Outgoing network capacity
     */
    NETWORK_OUT_RATE("NwOutRate", "networkOutRateKB", "double"),

    /**
     * Leader inbound network rate
     */
    LEADER_NETWORK_IN_RATE("LeaderNwInRate", "leaderNetworkInRateKB", "double"),

    /**
     * Follower inbound network rate
     */
    FOLLOWER_NETWORK_IN_RATE("FollowerNwInRate", "followerNetworkInRateKB", "double"),

    /**
     * Potential maximum outgoing network rate
     */
    POTENTIAL_MAX_NETWORK_OUT_RATE("PnwOutRate", "potentialMaxNetworkOutRateKB", "double");

    /** The key used in the load JSON object returned by Cruise Control. */
    private final String cruiseControlKey;
    /** The key used for the KafkaRebalance status field. */
    private final String kafkaRebalanceStatusKey;
    /** The type of value stored in the relevant field. */
    private final String type;

    /**
     * Constructs an Enum with Cruise Control parameter
     *
     * @param cruiseControlKey          Key of the parameter
     * @param kafkaRebalanceStatusKey   Status key of the parameter
     * @param type                      Type of the parameter
     */
    CruiseControlLoadParameters(String cruiseControlKey, String kafkaRebalanceStatusKey, String type) {
        this.cruiseControlKey = cruiseControlKey;
        this.kafkaRebalanceStatusKey = kafkaRebalanceStatusKey;
        this.type = type;
    }

    /**
     * @return  The parameter key
     */
    public String getCruiseControlKey() {
        return cruiseControlKey;
    }

    /**
     * @return  The status key
     */
    public String getKafkaRebalanceStatusKey() {
        return kafkaRebalanceStatusKey;
    }

    private String getType() {
        return type;
    }

    private static List<CruiseControlLoadParameters> filterByType(String filterType) {
        return Arrays.stream(CruiseControlLoadParameters.values())
                .filter(loadParameter -> filterType.equals(loadParameter.getType()))
                .collect(Collectors.toList());
    }

    /**
     * @return  List with all integer parameters
     */
    public static List<CruiseControlLoadParameters> getIntegerParameters() {
        return filterByType("int");
    }

    /**
     * @return  List with all double parameters
     */
    public static List<CruiseControlLoadParameters> getDoubleParameters() {
        return filterByType("double");
    }

}
