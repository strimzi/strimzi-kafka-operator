/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of the Cruise Control broker capacity settings.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"disk", "cpuUtilization", "cpu", "inboundNetwork", "outboundNetwork", "overrides"})
@EqualsAndHashCode
@ToString
public class BrokerCapacity implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private String disk;
    private Integer cpuUtilization;
    private String cpu;
    private String inboundNetwork;
    private String outboundNetwork;
    private List<BrokerCapacityOverride> overrides;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Deprecated
    @DeprecatedProperty(description = "The Cruise Control disk capacity setting has been deprecated, is ignored, and will be removed in the future")
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

    @Deprecated
    @DeprecatedProperty(description = "The Cruise Control CPU capacity setting has been deprecated, is ignored, and will be removed in the future")
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
    @Pattern("^[0-9]+([.][0-9]{0,3}|[m]?)$")
    @Description("Broker capacity for CPU resource in cores or millicores. " +
            "For example, 1, 1.500, 1500m. " +
            "For more information on valid CPU resource units see https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu")
    public String getCpu() {
        return cpu;
    }

    public void setCpu(String cpu) {
        this.cpu = cpu;
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

    @JsonInclude(content = JsonInclude.Include.NON_NULL, value = JsonInclude.Include.NON_EMPTY)
    @Description("Overrides for individual brokers. " +
            "The `overrides` property lets you specify a different capacity configuration for different brokers.")
    public List<BrokerCapacityOverride> getOverrides() {
        return overrides;
    }

    public void setOverrides(List<BrokerCapacityOverride> overrides) {
        this.overrides = overrides;
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
