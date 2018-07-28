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

import static java.util.Collections.emptyMap;

/**
 * Configures the broker authorization
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaListeners implements Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaTlsListener tls;
    private KafkaPlainListener plain;
    private Map<String, Object> additionalProperties;

    @Description("Configures TLS listener on port 9093.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaTlsListener getTls() {
        return tls;
    }

    public void setTls(KafkaTlsListener tls) {
        this.tls = tls;
    }

    @Description("Configures plain listener on port 9092.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaPlainListener getPlain() {
        return plain;
    }

    public void setPlain(KafkaPlainListener plain) {
        this.plain = plain;
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
