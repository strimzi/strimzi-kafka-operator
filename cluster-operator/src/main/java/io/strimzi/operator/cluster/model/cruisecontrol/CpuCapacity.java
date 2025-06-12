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
