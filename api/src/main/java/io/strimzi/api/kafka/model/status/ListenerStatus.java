/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represents a single listener
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "addresses" })
@EqualsAndHashCode
public class ListenerStatus implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private String type;
    private List<ListenerAddress> addresses;
    private Map<String, Object> additionalProperties;

    @Description("The type of the listener. " +
            "Can be one of the following three types: `plain`, `tls`, and `external`.")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Description("A list of the addresses for this listener.")
    public List<ListenerAddress> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<ListenerAddress> addresses) {
        this.addresses = addresses;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
