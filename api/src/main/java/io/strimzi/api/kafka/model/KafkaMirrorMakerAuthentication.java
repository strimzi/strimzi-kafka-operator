/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures the Kafka Mirror Maker authentication
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaMirrorMakerAuthenticationTls.TYPE_TLS, value = KafkaMirrorMakerAuthenticationTls.class),
        @JsonSubTypes.Type(name = KafkaMirrorMakerAuthenticationScramSha512.TYPE_SCRAM_SHA_512, value = KafkaMirrorMakerAuthenticationScramSha512.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class KafkaMirrorMakerAuthentication implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authentication type. " +
            "Currently the only supported types are `tls` and `scram-sha-512`. " +
            "`scram-sha-512` type uses SASL SCRAM-SHA-512 Authentication. " +
            "The `tls` type uses TLS Client Authentication. " +
            "The `tls` type is supported only over TLS connections.")
    @JsonIgnore
    public abstract String getType();

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
