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
@JsonPropertyOrder({"diskMiB", "cpuUtilization", "inboundNetworkKiBPerSecond", "outboundNetworkKiBPerSecond"})
@EqualsAndHashCode
public class BrokerCapacity implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private Integer diskMiB;
    private Integer cpuUtilization;
    private Integer inboundNetworkKiBPerSecond;
    private Integer outboundNetworkKiBPerSecond;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Broker capacity for disk in base 2 mebibytes.")
    public Integer getDiskMiB() {
        return diskMiB;
    }

    public void setDiskMiB(Integer diskMiB) {
        this.diskMiB = diskMiB;
    }

    @Minimum(0)
    @Maximum(100)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Broker capacity for CPU resource utilization as a percentage (0 - 100).")
    public Integer getCpuUtilization() {
        return cpuUtilization;
    }

    public void setCpuUtilization(Integer cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Broker capacity for network inbound throughput in base 2 kibibytes per second.")
    public Integer getInboundNetworkKiBPerSecond() {
        return inboundNetworkKiBPerSecond;
    }

    public void setInboundNetworkKiBPerSecond(Integer inboundNetworkKiBPerSecond) {
        this.inboundNetworkKiBPerSecond = inboundNetworkKiBPerSecond;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Broker capacity for network outbound throughput in base 2 kibibytes per second.")
    public Integer getOutboundNetworkKiBPerSecond() {
        return outboundNetworkKiBPerSecond;
    }

    public void setOutboundNetworkKiBPerSecond(Integer outboundNetworkKiBPerSecond) {
        this.outboundNetworkKiBPerSecond = outboundNetworkKiBPerSecond;
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
