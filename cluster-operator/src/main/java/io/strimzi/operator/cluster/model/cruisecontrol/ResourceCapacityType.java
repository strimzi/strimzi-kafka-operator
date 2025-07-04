/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;

import java.util.function.Function;

import static io.strimzi.operator.cluster.model.cruisecontrol.CpuCapacity.DEFAULT_CPU_CORE_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.DiskCapacity.DEFAULT_DISK_CAPACITY_IN_MIB;
import static io.strimzi.operator.cluster.model.cruisecontrol.NetworkCapacity.DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND;

/**
 * Enum representing the various types of resource capacities used in Cruise Control configuration.
 * Each enum constant maps to a specific resource type (e.g. CPU, Disk, Inbound Network, and Outbound Network),
 * with methods for retrieving configured values from both default broker capacity and broker-specific overrides.
 */
public enum ResourceCapacityType {
    /**
     * Disk capacity configuration.
     * <p>
     * Disk capacity configuration is handled automatically by Strimzi, there is no need for cluster or broker-specific overrides.
     */
    DISK(null, null, DEFAULT_DISK_CAPACITY_IN_MIB),

    /**
     * CPU capacity configuration.
     */
    CPU(BrokerCapacity::getCpu, BrokerCapacityOverride::getCpu, DEFAULT_CPU_CORE_CAPACITY),

    /**
     * Inbound network capacity configuration.
     */
    INBOUND_NETWORK(BrokerCapacity::getInboundNetwork, BrokerCapacityOverride::getInboundNetwork, DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND),

    /**
     * Outbound network capacity configuration.
     */
    OUTBOUND_NETWORK(BrokerCapacity::getOutboundNetwork, BrokerCapacityOverride::getOutboundNetwork, DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND);

    private final Function<BrokerCapacity, String> generalResourceCapacityGetter;
    private final Function<BrokerCapacityOverride, String> overriddenResourceCapacityGetter;
    private final String defaultResourceCapacity;

    ResourceCapacityType(Function<BrokerCapacity, String> generalResourceCapacityGetter,
                         Function<BrokerCapacityOverride, String> overriddenResourceCapacityGetter,
                         String defaultResourceCapacity
    ) {
        this.generalResourceCapacityGetter = generalResourceCapacityGetter;
        this.overriddenResourceCapacityGetter = overriddenResourceCapacityGetter;
        this.defaultResourceCapacity = defaultResourceCapacity;
    }

    private String getGeneralResourceCapacity(BrokerCapacity brokerCapacity) {
        return generalResourceCapacityGetter.apply(brokerCapacity);
    }

    private String getOverriddenResourceCapacity(BrokerCapacityOverride override) {
        return overriddenResourceCapacityGetter.apply(override);
    }

    /**
     * @return The default capacity value for given resource.
     */
    public String getDefaultResourceCapacity() {
        return defaultResourceCapacity;
    }

    /**
     * Given the configured brokerCapacity, broker-specific capacity override, and broker resource requirements,
     * returns the capacity for the resource.
     *
     * <p>
     * The broker-specific capacity override takes top precedence, then general brokerCapacity configuration,
     * and then the Kafka resource requests (only for CPU), then the Kafka resource limits (only for CPU), then resource
     * default.
     *
     * For example:
     * <ul>
     *   <li> (1) The brokerCapacityOverride for a specific broker.
     *   <li> (2) The general brokerCapacity configuration.
     *   <li> (3) Kafka resource requests (CPU specific)
     *   <li> (4) Kafka resource limits   (CPU specific)
     *   <li> (5) The resource default.
     * </ul>
     *
     * @param brokerCapacity         The general brokerCapacity configuration.
     * @param brokerCapacityOverride The brokerCapacityOverride for specific broker.
     * @param resourceRequirements   The Kafka resource requests and limits (for all brokers).
     *
     * @return The capacity of resource represented as a String.
     */
    public String processResourceCapacity(BrokerCapacity brokerCapacity,
                                          BrokerCapacityOverride brokerCapacityOverride,
                                          ResourceRequirements resourceRequirements) {
        if (brokerCapacityOverride != null && getOverriddenResourceCapacity(brokerCapacityOverride) != null) {
            return getOverriddenResourceCapacity(brokerCapacityOverride);
        }

        if (brokerCapacity != null && getGeneralResourceCapacity(brokerCapacity) != null) {
            return getGeneralResourceCapacity(brokerCapacity);
        }

        if (this == CPU) {
            String cpuBasedOnRequirements = ResourceRequirementsUtils.getCpuBasedOnRequirements(resourceRequirements);
            if (cpuBasedOnRequirements != null) {
                return cpuBasedOnRequirements;
            }
        }

        return getDefaultResourceCapacity();
    }
}
