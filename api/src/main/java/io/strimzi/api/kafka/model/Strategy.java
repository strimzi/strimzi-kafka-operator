/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

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
 * A representation of the configurable aspect of a strategy (used in deployment config).
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonPropertyOrder({"type"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Strategy implements Serializable {
    private static final long serialVersionUID = 1L;

    private String type;

    public static final String TYPE_RECREATE = "Recreate";
    public static final String TYPE_ROLLING = "Rolling";
    public static final String TYPE_CUSTOM = "Custom";
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Deployment Strategy")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
