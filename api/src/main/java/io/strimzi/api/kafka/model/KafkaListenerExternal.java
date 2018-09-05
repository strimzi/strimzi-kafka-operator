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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Configures the external listener which exposes Kafka outside of OpenShift
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaListenerExternalRoute.TYPE_ROUTE, value = KafkaListenerExternalRoute.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class KafkaListenerExternal implements Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaListenerAuthentication serverAuthentication;
    private Map<String, Object> additionalProperties;

    @Description("Type of the external listener. " +
            "Currently the only supported type is `route`. " +
            "`route` type uses OpenShift Route for exposing Kafka to the outside.")
    @JsonIgnore
    public abstract String getType();

    @Description("Authorization configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaListenerAuthentication getAuthentication() {
        return serverAuthentication;
    }

    public void setAuthentication(KafkaListenerAuthentication serverAuthentication) {
        this.serverAuthentication = serverAuthentication;
    }

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
