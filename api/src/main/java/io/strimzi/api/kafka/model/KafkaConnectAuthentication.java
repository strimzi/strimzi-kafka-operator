/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;

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
        @JsonSubTypes.Type(name = KafkaConnectAuthenticationPlain.TYPE_PLAIN, value = KafkaConnectAuthenticationPlain.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public abstract class KafkaConnectAuthentication implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authentication type. " +
            "Currently the only supported types are `tls`, `scram-sha-512` and `plain`. " +
            "`scram-sha-512` type uses SASL SCRAM-SHA-512 Authentication. " +
            "`plain` type uses SASL PLAIN Authentication. " +
            "`tls` type uses TLS Client Authentication. " +
            "`tls` type is supported only over TLS connections.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
