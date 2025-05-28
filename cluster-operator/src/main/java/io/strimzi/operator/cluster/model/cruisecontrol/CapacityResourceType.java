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

/**
 * Enum representing the various types of resource capacities used in Cruise Control configuration.
 * Each enum constant maps to a specific resource type (e.g. CPU, Disk, Network), with methods for
 * retrieving configured values from both default broker capacity and broker-specific overrides.
 */
public enum CapacityResourceType {
    /**
     * Disk capacity configuration.
     * <p>
     * Note: Disk capacity does not support default values and broker-specific overrides.
     */
    DISK("DISK",  null, null),

    /**
     * CPU capacity configuration.
     * <p>
     * Supports default values and broker-specific overrides.
     */
    CPU("CPU", BrokerCapacity::getCpu, BrokerCapacityOverride::getCpu),

    /**
     * Inbound network capacity configuration.
     * <p>
     * Supports default values and broker-specific overrides.
     */
    INBOUND_NETWORK("NW_IN", BrokerCapacity::getInboundNetwork, BrokerCapacityOverride::getInboundNetwork),

    /**
     * Outbound network capacity configuration.
     * <p>
     * Supports default values and broker-specific overrides.
     */
    OUTBOUND_NETWORK("NW_OUT", BrokerCapacity::getOutboundNetwork, BrokerCapacityOverride::getOutboundNetwork);

    private final String key;
    private final Function<BrokerCapacity, Object> defaultCapacityGetter;
    private final Function<BrokerCapacityOverride, Object> defaultOverrideGetter;

    CapacityResourceType(String key,
                         Function<BrokerCapacity, Object> brokerDefaultGetter,
                         Function<BrokerCapacityOverride, Object> overrideGetter) {
        this.key = key;
        this.defaultCapacityGetter = brokerDefaultGetter;
        this.defaultOverrideGetter = overrideGetter;
    }

    /**
     * Returns the string key associated with this resource type, as used in Cruise Control capacity JSON
     * (e.g., "CPU", "DISK", "NW_IN", "NW_OUT").
     *
     * @return the string key identifying this resource type
     */
    public String getKey() {
        return key;
    }

    private Object getCapacityDefault(BrokerCapacity brokerCapacity) {
        return defaultCapacityGetter.apply(brokerCapacity);
    }

    private Object getCapacityOverride(BrokerCapacityOverride override) {
        return defaultOverrideGetter.apply(override);
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
     * @param kafkaBrokerResources A map of broker IDs to their Kubernetes resource requirements
     * @return {@code true} if the capacity for this resource type is considered configured, otherwise {@code false}
     */
    public boolean isCapacityConfigured(BrokerCapacity brokerCapacity, Map<String, ResourceRequirements> kafkaBrokerResources) {
        if (this == CPU && Capacity.cpuRequestsMatchLimits(kafkaBrokerResources)) {
            return true;
        }

        if (brokerCapacity == null) {
            return false;
        }

        if (getCapacityDefault(brokerCapacity) != null) {
            return true;
        }

        List<BrokerCapacityOverride> overrides = brokerCapacity.getOverrides();
        return overrides != null && overrides.stream().allMatch(o -> getCapacityOverride(o) != null);
    }
}
