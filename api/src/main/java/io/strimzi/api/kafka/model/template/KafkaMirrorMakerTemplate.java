/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a template for Kafka Mirror Maker resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "deployment", "pod"})
public class KafkaMirrorMakerTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private ResourceTemplate deployment;
    private ResourceTemplate pod;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka Mirror Maker `Deployment`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(ResourceTemplate deployment) {
        this.deployment = deployment;
    }

    @Description("Template for Kafka Mirror Maker `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPod() {
        return pod;
    }

    public void setPod(ResourceTemplate pod) {
        this.pod = pod;
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
