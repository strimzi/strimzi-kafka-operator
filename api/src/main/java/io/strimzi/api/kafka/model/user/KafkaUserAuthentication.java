/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user;

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

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = KafkaUserTlsClientAuthentication.TYPE_TLS, value = KafkaUserTlsClientAuthentication.class),
    @JsonSubTypes.Type(name = KafkaUserTlsExternalClientAuthentication.TYPE_TLS_EXTERNAL, value = KafkaUserTlsExternalClientAuthentication.class),
    @JsonSubTypes.Type(name = KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512, value = KafkaUserScramSha512ClientAuthentication.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class KafkaUserAuthentication implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authentication type.")
    public abstract String getType();

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
