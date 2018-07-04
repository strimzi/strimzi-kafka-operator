/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.strimzi.crdgenerator.annotations.Type;
import io.sundr.builder.annotations.Buildable;

import java.util.HashMap;
import java.util.Map;


@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class CpuMemory {

    private String memory;
    private String milliCpu;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Buildable(
            editableEnabled = false,
            generateBuilderPackage = true,
            builderPackage = "io.strimzi.api.kafka.model"
    )
    public CpuMemory() {
    }

    public CpuMemory(long mem, int milliCpu) {
        this.memory = MemoryDeserializer.format(mem);
        this.milliCpu = MilliCpuDeserializer.format(milliCpu);
    }

    /** The memory in bytes */
    @JsonIgnore
    public long memoryAsLong() {
        return memory == null ? 0 : MemoryDeserializer.parse(memory);
    }

    public void memoryAsLong(long memory) {
        this.memory = MemoryDeserializer.format(memory);
    }

    /** The memory in Kubernetes syntax. */
    @Description("Memory")
    @Pattern("[0-9]+([kKmMgGtTpPeE]i?)?$")
    @Type("string")
    @JsonProperty("memory")
    public String getMemory() {
        return memory;
    }

    public void setMemory(String mem) {
        this.memory = mem;
    }


    /** The CPUs in "millicpus". */
    @JsonIgnore
    public int milliCpuAsInt() {
        return MilliCpuDeserializer.parse(milliCpu);
    }

    public void milliCpuAsInt(int milliCpu) {
        this.milliCpu = MilliCpuDeserializer.format(milliCpu);
    }

    /** The CPUs formatted using Kubernetes syntax. */
    @Description("CPU")
    @JsonProperty("cpu")
    @Type("string")
    @Pattern("[0-9]+m?$")
    public String getMilliCpu() {
        return this.milliCpu;
    }

    public void setMilliCpu(String milliCpu) {
        this.milliCpu = milliCpu;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
