/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures the external listener which exposes Kafka outside of Kubernetes / OpenShift
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaListenerExternalRoute.TYPE_ROUTE, value = KafkaListenerExternalRoute.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalLoadBalancer.TYPE_LOADBALANCER, value = KafkaListenerExternalLoadBalancer.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalNodePort.TYPE_NODEPORT, value = KafkaListenerExternalNodePort.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class KafkaListenerExternal implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Type of the external listener. " +
            "Currently the supported types are `route`, `loadbalancer`, and `nodeport`. \n\n" +
            "* `route` type uses OpenShift Routes to expose Kafka." +
            "* `loadbalancer` type uses LoadBalancer type services to expose Kafka." +
            "* `nodeport` type uses NodePort type services to expose Kafka.")
    public abstract String getType();

    @Description("Authentication configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("authentication")
    public abstract KafkaListenerAuthentication getAuth();

    public abstract void setAuth(KafkaListenerAuthentication auth);

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
