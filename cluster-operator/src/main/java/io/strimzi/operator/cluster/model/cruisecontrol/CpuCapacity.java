/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.operator.cluster.model.Quantities;
import io.vertx.core.json.JsonObject;

/**
 * Cruise Control CPU Capacity class
 */
public class CpuCapacity {
    private static final String CORES_KEY = "num.cores";

    private final String cores;

    /**
     * Constructor
     *
     * @param cores     CPU cores configuration
     */
    public CpuCapacity(String cores) {
        this.cores = milliCpuToCpu(Quantities.parseCpuAsMilliCpus(cores));
    }

    protected static String milliCpuToCpu(int milliCPU) {
        return String.valueOf(milliCPU / 1000.0);
    }

    /**
     * Returns CpuCapacity object as a JsonObject
     *
     * @return The CpuCapacity object as a JsonObject
     */
    public JsonObject getJson() {
        return new JsonObject().put(CORES_KEY, this.cores);
    }

    @Override
    public String toString() {
        return this.getJson().toString();
    }
}
