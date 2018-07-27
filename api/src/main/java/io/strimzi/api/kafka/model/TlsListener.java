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
import io.sundr.builder.annotations.Buildable;

/**
 * Configures the TLS listener of Kafka broker
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TlsListener implements Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaListenerAuthentication authentication;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

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
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
