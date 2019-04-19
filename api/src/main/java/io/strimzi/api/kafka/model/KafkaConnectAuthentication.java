/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures the Kafka Connect authentication
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaConnectAuthenticationTls.TYPE_TLS, value = KafkaConnectAuthenticationTls.class),
        @JsonSubTypes.Type(name = KafkaConnectAuthenticationScramSha512.TYPE_SCRAM_SHA_512, value = KafkaConnectAuthenticationScramSha512.class),
        @JsonSubTypes.Type(name = KafkaConnectAuthenticationSaslPlain.TYPE_SASL_PLAIN, value = KafkaConnectAuthenticationSaslPlain.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class KafkaConnectAuthentication implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authentication type. " +
            "Currently the only supported types are `tls`, `scram-sha-512` and `sasl-plain`. " +
            "`scram-sha-512` type uses SASL SCRAM-SHA-512 Authentication. " +
            "`sasl-plain` type uses SASL PLAIN Authentication. " +
            "`tls` type uses TLS Client Authentication. " +
            "`tls` type is supported only over TLS connections.")
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
