/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.balancing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Cruise Control broker capacity settings. Since the Kafka brokers
 * in Strimzi are homogeneous, the capacity values for each resource will be
 * used for every broker.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"disk", "cpu", "networkIn", "networkOut"})
@EqualsAndHashCode
public class BrokerCapacity implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private Integer disk;
    private Integer cpu;
    private Integer networkIn;
    private Integer networkOut;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Broker capacity for disk in megabytes.")
    public Integer getDisk() {
        return disk;
    }

    public void setDisk(Integer disk) {
        this.disk = disk;
    }

    @Minimum(0)
    @Maximum(100)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Broker capacity for CPU utilization as a percentage (0 - 100).")
    public Integer getCpu() {
        return cpu;
    }

    public void setCpu(Integer cpu) {
        this.cpu = cpu;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Broker capacity for network inbound throughput in kilobytes per second.")
    public Integer getNetworkIn() {
        return networkIn;
    }

    public void setNetworkIn(Integer networkIn) {
        this.networkIn = networkIn;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Broker capacity for network outbound throughput in kilobytes per second.")
    public Integer getNetworkOut() {
        return networkOut;
    }

    public void setNetworkOut(Integer networkOut) {
        this.networkOut = networkOut;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
