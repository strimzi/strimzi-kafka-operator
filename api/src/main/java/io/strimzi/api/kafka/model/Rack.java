/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the rack configuration.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Rack implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topologyKey;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    public Rack() {
    }

    public Rack(String topologyKey) {
        this.topologyKey = topologyKey;
    }

    @Description("A key that matches labels assigned to the OpenShift or Kubernetes cluster nodes. " +
            "The value of the label is used to set the broker's `broker.rack` config.")
    @Example("failure-domain.beta.kubernetes.io/zone")
    @JsonProperty(required = true)
    public String getTopologyKey() {
        return topologyKey;
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
