/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;

import java.util.List;
import java.util.Map;
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
     * Disk configuration is done automatically by Strimzi, there is no need for cluster or broker-specific overrides.
     */
    DISK("DISK", null, null, DEFAULT_DISK_CAPACITY_IN_MIB, CapacityConfiguration.CapacityEntry::disk),

    /**
     * CPU capacity configuration.
     */
    CPU("CPU", BrokerCapacity::getCpu, BrokerCapacityOverride::getCpu, DEFAULT_CPU_CORE_CAPACITY,  CapacityConfiguration.CapacityEntry::cpu),

    /**
     * Inbound network capacity configuration.
     */
    INBOUND_NETWORK("NW_IN", BrokerCapacity::getInboundNetwork, BrokerCapacityOverride::getInboundNetwork, DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND, CapacityConfiguration.CapacityEntry::inboundNetwork),

    /**
     * Outbound network capacity configuration.
     */
    OUTBOUND_NETWORK("NW_OUT", BrokerCapacity::getOutboundNetwork, BrokerCapacityOverride::getOutboundNetwork, DEFAULT_NETWORK_CAPACITY_IN_KIB_PER_SECOND, CapacityConfiguration.CapacityEntry::outboundNetwork);

    private final String key;
    private final Function<BrokerCapacity, String> generalResourceCapacityGetter;
    private final Function<BrokerCapacityOverride, String> overriddenResourceCapacityGetter;
    private final String defaultResourceCapacity;
    private final Function<CapacityConfiguration.CapacityEntry, ?> entryGetter;

    ResourceCapacityType(String key,
                         Function<BrokerCapacity, String> generalResourceCapacityGetter,
                         Function<BrokerCapacityOverride, String> overriddenResourceCapacityGetter,
                         String defaultResourceCapacity,
                         Function<CapacityConfiguration.CapacityEntry, ?> entryGetter) {
        this.key = key;
        this.entryGetter = entryGetter;
        this.generalResourceCapacityGetter = generalResourceCapacityGetter;
        this.overriddenResourceCapacityGetter = overriddenResourceCapacityGetter;
        this.defaultResourceCapacity = defaultResourceCapacity;
    }

    /**
     * Returns the string key associated with this resource type, as used in Cruise Control capacity JSON
     * (e.g., "CPU", "DISK", "NW_IN", "NW_OUT").
     *
     * @return the string key identifying this resource type.
     */
    public String getKey() {
        return key;
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
     * and then the Kafka resource requests, then the Kafka resource limits.
     *
     * For example:
     * <ul>
     *   <li> (1) The brokerCapacityOverride for a specific broker.
     *   <li> (2) The general brokerCapacity configuration.
     *   <li> (3) Kafka resource requests (CPU specific)
     *   <li> (4) Kafka resource limits   (CPU specific)
     * </ul>
     * When none of Cruise Control capacity configurations mentioned above are configured, capacity will be set to
     * resource defaults.
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
            String cpuBasedOnRequirements = CpuCapacity.getCpuBasedOnRequirements(resourceRequirements);
            if (cpuBasedOnRequirements != null) {
                return cpuBasedOnRequirements;
            }
        }

        return getDefaultResourceCapacity();
    }

    /**
     * Checks whether this resource type has its capacity properly configured.
     * <p>
     * The configuration can come from:
     * <ul>
     *     <li>The default capacity defined in the {@link BrokerCapacity} section of the Cruise Control spec</li>
     *     <li>Broker-specific overrides, where each override must define a value for this resource type</li>
     *     <li>For {@link #CPU}, it may also be configured via Kafka custom resource requests and limits.
     * </ul>
     *
     * @param brokerCapacity The brokerCapacity section of the Cruise Control specification from the Kafka custom resource.
     * @param kafkaBrokerResources A map of broker IDs to their Kubernetes resource requirements.
     * @return {@code true} if the capacity for this resource type is considered configured, otherwise {@code false.}
     */
    public boolean isCapacityConfigured(BrokerCapacity brokerCapacity, Map<String, ResourceRequirements> kafkaBrokerResources) {
        if (brokerCapacity != null) {
            if (getGeneralResourceCapacity(brokerCapacity) != null) {
                return true;
            }

            List<BrokerCapacityOverride> overrides = brokerCapacity.getOverrides();
            if (overrides != null && overrides.stream().allMatch(o -> getOverriddenResourceCapacity(o) != null)) {
                return true;
            }
        }

        if (this == CPU && CpuCapacity.cpuRequestsMatchLimits(kafkaBrokerResources)) {
            return true;
        }

        return false;
    }

    /**
     * Checks whether this resource type has its capacityConfiguration homogeneously configured across all brokers.
     *
     * @param capacityConfiguration The Cruise Control capacityConfiguration object.
     * @return {@code true} if capacityConfiguration is homogeneously configured; {@code false} otherwise.
     */
    public boolean isCapacityHomogeneouslyConfigured(CapacityConfiguration capacityConfiguration) {
        return capacityConfiguration.getCapacityEntries().values().stream()
                .map(entryGetter)
                .distinct()
                .limit(2)
                .count() == 1;
    }
}
