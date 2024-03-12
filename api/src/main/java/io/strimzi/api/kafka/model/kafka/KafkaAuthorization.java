/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures the broker authorization
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = KafkaAuthorizationSimple.TYPE_SIMPLE, value = KafkaAuthorizationSimple.class),
    @JsonSubTypes.Type(name = KafkaAuthorizationOpa.TYPE_OPA, value = KafkaAuthorizationOpa.class),
    @JsonSubTypes.Type(name = KafkaAuthorizationKeycloak.TYPE_KEYCLOAK, value = KafkaAuthorizationKeycloak.class),
    @JsonSubTypes.Type(name = KafkaAuthorizationCustom.TYPE_CUSTOM, value = KafkaAuthorizationCustom.class)
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class KafkaAuthorization implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authorization type. " +
            "Currently, the supported types are `simple`, `keycloak`, `opa` and `custom`. " +
            "`simple` authorization type uses Kafka's built-in authorizer for authorization. " +
            "`keycloak` authorization type uses Keycloak Authorization Services for authorization. " +
            "`opa` authorization type uses Open Policy Agent based authorization." +
            "`custom` authorization type uses user-provided implementation for authorization.")
    public abstract String getType();

    /**
     * Indicates whether the Authorizer supports the Kafka Admin API to manage ACLs or not.
     *
     * @return True if ACLs can be managed using Kafka Admin API. False otherwise.
     */
    public abstract boolean supportsAdminApi();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
