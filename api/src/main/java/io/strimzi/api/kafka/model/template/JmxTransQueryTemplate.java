/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({
        "targetMBean", "attributes", "outputs"})
@EqualsAndHashCode
public class JmxTransQueryTemplate implements Serializable, UnknownPropertyPreserving {
    private String targetMBean;
    private List<String> attributes;
    private List<String> outputs;

    private static final long serialVersionUID = 1L;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("If using a wildcard, the MBean(s) hat the data will be gathered from.")
    public String getTargetMBean() {
        return targetMBean;
    }

    public void setTargetMBean(String targetMBean) {
        this.targetMBean = targetMBean;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("attributes determine which attributes of the targeted MBean will be read")
    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("outputs is a list of names of the Output Definitions which configures where the data will be pushed " +
            "to and what data format it will be in.")
    public List<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<String> outputs) {
        this.outputs = outputs;
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