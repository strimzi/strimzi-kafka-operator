/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.api.kafka.model.type.SslClientAuthType;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;

/**
 * Configures the broker authorization
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaAuthentication implements Serializable {
    private static final long serialVersionUID = 1L;

    private SslClientAuthType tlsClientAuthentication;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Configures TLS Client Authentication on port 9093. Valid values are `required`, `requested` and `none`. When not specified, the default value will be `none`.")
    @Example("required")
    public SslClientAuthType getTlsClientAuthentication() {
        return tlsClientAuthentication;
    }

    public void setTlsClientAuthentication(SslClientAuthType tlsClientAuthentication) {
        this.tlsClientAuthentication = tlsClientAuthentication;
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
