/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures listener authentication.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = KafkaListenerAuthenticationTls.TYPE_TLS, value = KafkaListenerAuthenticationTls.class),
    @JsonSubTypes.Type(name = KafkaListenerAuthenticationScramSha512.SCRAM_SHA_512, value = KafkaListenerAuthenticationScramSha512.class),
    @JsonSubTypes.Type(name = KafkaListenerAuthenticationOAuth.TYPE_OAUTH, value = KafkaListenerAuthenticationOAuth.class),
    @JsonSubTypes.Type(name = KafkaListenerAuthenticationCustom.TYPE_CUSTOM, value = KafkaListenerAuthenticationCustom.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
@ToString
public abstract class KafkaListenerAuthentication implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Authentication type. " +
            "`oauth` type uses SASL OAUTHBEARER Authentication. " +
            "`scram-sha-512` type uses SASL SCRAM-SHA-512 Authentication. " +
            "`tls` type uses TLS Client Authentication. " +
            "`tls` type is supported only on TLS listeners." +
            "`custom` type allows for any authentication type to be used.")
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
