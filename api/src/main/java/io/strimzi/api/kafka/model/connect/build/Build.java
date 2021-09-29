/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation a Kafka Connect build to add additional connectors
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({ "pullSecret", "output", "connectorPlugins", "resources" })
@DescriptionFile
@EqualsAndHashCode
public class Build implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private String pullSecret;
    private Output output;
    private List<Plugin> plugins;
    private ResourceRequirements resources;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Secret used to pull base image")
    public String getPullSecret() {
        return pullSecret;
    }

    public void setPullSecret(String pullSecret) {
        this.pullSecret = pullSecret;
    }

    @Description("Configures where should the newly built image be stored. " +
            "Required")
    @JsonProperty(required = true)
    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    @Description("List of connector plugins which should be added to the Kafka Connect. " +
            "Required")
    @JsonProperty(required = true)
    public List<Plugin> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<Plugin> plugins) {
        this.plugins = plugins;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    @Description("CPU and memory resources to reserve for the build.")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
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

