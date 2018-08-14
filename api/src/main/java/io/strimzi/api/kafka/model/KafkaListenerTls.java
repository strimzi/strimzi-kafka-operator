/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures the TLS listener of Kafka broker
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaListenerTls implements Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaListenerAuthentication serverAuthentication;
    private Map<String, Object> additionalProperties;

    @Description("Authentication configuration for this listener.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaListenerAuthentication getAuthentication() {
        return serverAuthentication;
    }

    public void setAuthentication(KafkaListenerAuthentication serverAuthentication) {
        this.serverAuthentication = serverAuthentication;
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
