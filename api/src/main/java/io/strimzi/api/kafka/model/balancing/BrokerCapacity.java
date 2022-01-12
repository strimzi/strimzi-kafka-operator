/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.balancing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
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
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"disk", "cpuUtilization", "inboundNetwork", "outboundNetwork"})
@EqualsAndHashCode
public class BrokerCapacity implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private String disk;
    private Integer cpuUtilization;
    private String inboundNetwork;
    private String outboundNetwork;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Pattern("^[0-9]+([.][0-9]*)?([KMGTPE]i?|e[0-9]+)?$")
    @Description("Broker capacity for disk in bytes. " +
            "Use a number value with either standard Kubernetes byte units (K, M, G, or T), their bibyte (power of two) equivalents (Ki, Mi, Gi, or Ti), or a byte value with or without E notation. " +
            "For example, 100000M, 100000Mi, 104857600000, or 1e+11.")
    public String getDisk() {
        return disk;
    }

    public void setDisk(String disk) {
        this.disk = disk;
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
    @Pattern("^[0-9]+([KMG]i?)?B/s$")
    @Description("Broker capacity for inbound network throughput in bytes per second. " +
            "Use an integer value with standard Kubernetes byte units (K, M, G) or their bibyte (power of two) equivalents (Ki, Mi, Gi) per second. " +
            "For example, 10000KiB/s.")
    public String getInboundNetwork() {
        return inboundNetwork;
    }

    public void setInboundNetwork(String inboundNetwork) {
        this.inboundNetwork = inboundNetwork;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Pattern("^[0-9]+([KMG]i?)?B/s$")
    @Description("Broker capacity for outbound network throughput in bytes per second. " +
            "Use an integer value with standard Kubernetes byte units (K, M, G) or their bibyte (power of two) equivalents (Ki, Mi, Gi) per second. " +
            "For example, 10000KiB/s.")
    public String getOutboundNetwork() {
        return outboundNetwork;
    }

    public void setOutboundNetwork(String outboundNetwork) {
        this.outboundNetwork = outboundNetwork;
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
