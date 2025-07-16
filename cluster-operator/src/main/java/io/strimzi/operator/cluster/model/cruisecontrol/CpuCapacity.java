/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;
import io.strimzi.operator.cluster.model.Quantities;
import io.vertx.core.json.JsonObject;

/**
 * Cruise Control CPU capacity configuration for broker.
 */
public class CpuCapacity {
    /**
     * Default capacity value
     */
    /* test */ static final String DEFAULT_CPU_CORE_CAPACITY = "1.0";

    private static final String CORES_KEY = "num.cores";

    private final JsonObject config = new JsonObject();

    /**
     * Constructor
     *
     * Given the configured brokerCapacity, broker-specific capacity override, and broker resource requirements,
     * returns the capacity for the resource.
     *
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     * @param resourceRequirements   The Kafka resource requests and limits (for all brokers).
     */
    protected CpuCapacity(BrokerCapacity brokerCapacity,
                       BrokerCapacityOverride brokerCapacityOverride,
                       ResourceRequirements resourceRequirements) {
        String cores = processResourceCapacity(brokerCapacity, brokerCapacityOverride, resourceRequirements);
        config.put(CORES_KEY,  milliCpuToCpu(Quantities.parseCpuAsMilliCpus(cores)));
    }

    private static String milliCpuToCpu(int milliCPU) {
        return String.valueOf(milliCPU / 1000.0);
    }

    /**
     * Returns capacity value as a JsonObject.
     *
     * @return The capacity value as a JsonObject.
     */
    protected JsonObject getJson() {
        return config;
    }

    /**
     * Given the configured brokerCapacity, broker-specific capacity override, and broker resource requirements,
     * returns the capacity for the resource.
     *
     * <p>
     * The broker-specific capacity override takes top precedence, then general brokerCapacity configuration,
     * and then the Kafka resource requests, then the Kafka resource limits, then resource
     * default.
     *
     * For example:
     * <ul>
     *   <li> (1) The brokerCapacityOverride for a specific broker.
     *   <li> (2) The general brokerCapacity configuration.
     *   <li> (3) Kafka resource requests
     *   <li> (4) Kafka resource limits
     *   <li> (5) The resource default.
     * </ul>
     *
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     * @param resourceRequirements   The Kafka resource requests and limits (for all brokers).
     *
     * @return The capacity of resource represented as a String.
     */
    public static String processResourceCapacity(BrokerCapacity brokerCapacity,
                                                 BrokerCapacityOverride brokerCapacityOverride,
                                                 ResourceRequirements resourceRequirements) {
        if (brokerCapacityOverride != null && brokerCapacityOverride.getCpu() != null) {
            return brokerCapacityOverride.getCpu();
        } else if (brokerCapacity != null && brokerCapacity.getCpu() != null) {
            return brokerCapacity.getCpu();
        } else {
            String cpuBasedOnRequirements = getCpuBasedOnRequirements(resourceRequirements);
            if (cpuBasedOnRequirements != null) {
                return cpuBasedOnRequirements;
            } else {
                return DEFAULT_CPU_CORE_CAPACITY;
            }
        }
    }

    /**
     * Derives the CPU capacity from the resource requirements section of Strimzi custom resource.
     *
     * @param resources The Strimzi custom resource requirements containing CPU requests and/or limits.
     * @return The CPU capacity as a {@link String}, or {@code null} if no CPU values are defined.
     */
    private static String getCpuBasedOnRequirements(ResourceRequirements resources) {
        if (resources != null && resources.getRequests() != null && resources.getRequests().get("cpu") != null) {
            return resources.getRequests().get("cpu").toString();
        } else if (resources != null && resources.getLimits() != null && resources.getLimits().get("cpu") != null) {
            return resources.getLimits().get("cpu").toString();
        } else {
            return null;
        }
    }
}
