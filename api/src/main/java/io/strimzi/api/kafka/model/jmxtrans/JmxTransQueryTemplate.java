/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.jmxtrans;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"targetMBean", "attributes", "outputs"})
@EqualsAndHashCode
@ToString
public class JmxTransQueryTemplate implements UnknownPropertyPreserving {
    private String targetMBean;
    private List<String> attributes;
    private List<String> outputs;
    private Map<String, Object> additionalProperties;

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("If using wildcards instead of a specific MBean then the data is gathered from multiple MBeans. " +
            "Otherwise if specifying an MBean then data is gathered from that specified MBean.")
    public String getTargetMBean() {
        return targetMBean;
    }

    public void setTargetMBean(String targetMBean) {
        this.targetMBean = targetMBean;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Determine which attributes of the targeted MBean should be included")
    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("List of the names of output definitions specified in the spec.kafka.jmxTrans.outputDefinitions that have defined where JMX metrics are pushed to, and in which data format")
    public List<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<String> outputs) {
        this.outputs = outputs;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
