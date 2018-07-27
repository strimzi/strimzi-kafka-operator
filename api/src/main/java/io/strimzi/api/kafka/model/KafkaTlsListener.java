/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.sundr.builder.annotations.Buildable;

import static java.util.Collections.emptyMap;

/**
 * Configures the TLS listener of Kafka broker
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "authentication" })
public class KafkaTlsListener implements Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaListenerAuthentication authentication;
    private Map<String, Object> additionalProperties;

    @Description("Authentication configuration for Kafka's TLS listener")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaListenerAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaListenerAuthentication authentication) {
        this.authentication = authentication;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
