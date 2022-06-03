/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.operator.cluster.operator.resource.Quantities;
import io.vertx.core.json.JsonObject;

public class CpuCapacity {
    private static final String CORES_KEY = "num.cores";

    private String cores;

    public CpuCapacity(String cores) {
        this.cores = milliCputoCpu(Quantities.parseCpuAsMilliCpus(cores));
    }

    public static String milliCputoCpu(int milliCPU) {
        return String.valueOf(milliCPU / 1000.0);
    }

    public JsonObject getJson() {
        return new JsonObject().put(CORES_KEY, this.cores);
    }

    public String toString() {
        return this.getJson().toString();
    }
}
