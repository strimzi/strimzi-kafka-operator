/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a template for JmxTrans resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "jmxTransContainer", "additionalProperties" })
@EqualsAndHashCode
public class JmxTransTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private ContainerTemplate jmxTransContainer;
    protected Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for JmxTrans container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getJmxTransContainer() {
        return jmxTransContainer;
    }

    public void setJmxTransContainer(ContainerTemplate tlsSidecarContainer) {
        this.jmxTransContainer = tlsSidecarContainer;
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
