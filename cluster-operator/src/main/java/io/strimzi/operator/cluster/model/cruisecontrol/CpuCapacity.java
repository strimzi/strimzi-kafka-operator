/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;
import io.strimzi.operator.cluster.model.Quantities;
import io.strimzi.operator.common.model.resourcerequirements.ResourceRequirementsUtils;
import io.vertx.core.json.JsonObject;

import java.util.Map;

import static io.strimzi.operator.cluster.model.cruisecontrol.ResourceCapacityType.CPU;

/**
 * Cruise Control CPU capacity configuration for broker.
 */
public class CpuCapacity {
    protected static final String DEFAULT_CPU_CORE_CAPACITY = "1.0";
    /**
     * Key used to identify resource in broker entry in Cruise Control capacity configuration.
     */
    public static final String KEY = "CPU";

    private static final String CORES_KEY = "num.cores";

    private final JsonObject config = new JsonObject();

    /**
     * Constructor
     *
     * @param cores     CPU cores configuration
     */
    public CpuCapacity(String cores) {
        config.put(CORES_KEY, milliCpuToCpu(Quantities.parseCpuAsMilliCpus(cores)));
    }

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
    public CpuCapacity(BrokerCapacity brokerCapacity,
                        BrokerCapacityOverride brokerCapacityOverride,
                        ResourceRequirements resourceRequirements) {
        this(CPU.processResourceCapacity(brokerCapacity, brokerCapacityOverride, resourceRequirements));
    }

    private static String milliCpuToCpu(int milliCPU) {
        return String.valueOf(milliCPU / 1000.0);
    }

    /**
     * Checks whether all Kafka broker pods have their CPU resource requests equal to their CPU limits.
     *
     * @param kafkaBrokerResources a map of broker pod names to their {@link ResourceRequirements}
     * @return {@code true} if all brokers have matching CPU requests and limits; {@code false} otherwise
     */
    public static boolean cpuRequestsMatchLimits(Map<String, ResourceRequirements> kafkaBrokerResources) {
        if (kafkaBrokerResources == null) {
            return false;
        }
        for (ResourceRequirements resourceRequirements : kafkaBrokerResources.values()) {
            Quantity request = ResourceRequirementsUtils.getCpuRequest(resourceRequirements);
            Quantity limit = ResourceRequirementsUtils.getCpuLimit(resourceRequirements);
            if (request == null || limit == null || request.compareTo(limit) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Derives the CPU capacity from Kubernetes {@link ResourceRequirements}.
     * <p>
     * This method first attempts to extract the CPU request value from the resource requirements.
     * If the request is defined, it is converted from millicores to cores and returned as a {@link String}.
     * If no request is present, it falls back to the CPU limit (if available) and performs the same conversion.
     * If neither a request nor a limit is specified, {@code null} is returned.
     *
     * @param resourceRequirements The Kubernetes resource requirements containing CPU requests and/or limits.
     * @return The CPU capacity in cores as a {@link String}, or {@code null} if no CPU values are defined.
     */
    public static String getCpuBasedOnRequirements(ResourceRequirements resourceRequirements) {
        Quantity request = ResourceRequirementsUtils.getCpuRequest(resourceRequirements);
        Quantity limit = ResourceRequirementsUtils.getCpuLimit(resourceRequirements);

        if (request != null) {
            int milliCpus = Quantities.parseCpuAsMilliCpus(request.toString());
            return CpuCapacity.milliCpuToCpu(milliCpus);
        } else if (limit != null) {
            int milliCpus = Quantities.parseCpuAsMilliCpus(limit.toString());
            return CpuCapacity.milliCpuToCpu(milliCpus);
        } else {
            return null;
        }
    }

    /**
     * Returns capacity value as a JsonObject.
     *
     * @return The capacity value as a JsonObject.
     */
    public JsonObject getJson() {
        return config;
    }

    @Override
    public String toString() {
        return config.toString();
    }
}
