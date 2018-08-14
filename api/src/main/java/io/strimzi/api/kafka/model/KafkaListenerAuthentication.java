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
 * Configures listener authentication.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaListenerAuthenticationTls.TYPE_TLS, value = KafkaListenerAuthenticationTls.class),
        @JsonSubTypes.Type(name = KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512, value = KafkaListenerAuthenticationScramSha512.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class KafkaListenerAuthentication implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authentication type. " +
            "Currently the only supported type is `tls`. " +
            "`tls` type uses TLS Client Authentication. " +
            "`tls` type is supported only on TLS listeners.")
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
