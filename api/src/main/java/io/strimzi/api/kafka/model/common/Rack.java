/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the rack configuration.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"topologyKey"})
@EqualsAndHashCode
@ToString
public class Rack implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private String topologyKey;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    public Rack() {
    }

    public Rack(String topologyKey) {
        this.topologyKey = topologyKey;
    }

    @Description("A key that matches labels assigned to the Kubernetes cluster nodes. " +
            "The value of the label is used to set a broker's `broker.rack` config, and the `client.rack` config for Kafka Connect or MirrorMaker 2.")
    @Example("topology.kubernetes.io/zone")
    @JsonProperty(required = true)
    public String getTopologyKey() {
        return topologyKey;
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
