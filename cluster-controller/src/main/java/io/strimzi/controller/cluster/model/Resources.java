/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class Resources {

    public Resources() {
    }

    public Resources(CpuMemory limits, CpuMemory requests) {
        this.limits = limits;
        this.requests = requests;
    }

    public static class CpuMemory {
        private long memory;
        private int milliCpu;

        public CpuMemory() {
        }

        public CpuMemory(long memory, int milliCpu) {
            this.memory = memory;
            this.milliCpu = milliCpu;
        }

        @JsonDeserialize(using = MemoryDeserializer.class)
        public long getMemory() {
            return memory;
        }

        public void setMemory(long memory) {
            this.memory = memory;
        }

        /** The memory in Kubernetes syntax. */
        @JsonIgnore
        public String getMemoryFormatted() {
            return MemoryDeserializer.format(getMemory());
        }

        /** The CPUs in "millicpus". */
        @JsonDeserialize(using = MilliCpuDeserializer.class)
        @JsonProperty("cpu")
        public int getMilliCpu() {
            return milliCpu;
        }

        public void setMilliCpu(int milliCpu) {
            this.milliCpu = milliCpu;
        }

        /** The CPUs formatted using Kubernetes syntax. */
        @JsonIgnore
        public String getCpuFormatted() {
            return MilliCpuDeserializer.format(getMilliCpu());
        }
    }

    private CpuMemory limits;

    private CpuMemory requests;

    public CpuMemory getLimits() {
        return limits;
    }

    public void setLimits(CpuMemory limits) {
        this.limits = limits;
    }

    public CpuMemory getRequests() {
        return requests;
    }

    public void setRequests(CpuMemory requests) {
        this.requests = requests;
    }

    public static Resources fromJson(String json) {
        return JsonUtils.fromJson(json, Resources.class);
    }

    public String toString() {
        return JsonUtils.toJson(this);
    }
}
