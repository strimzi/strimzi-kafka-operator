/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures the Kafka client authentication in client based components
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = KafkaClientAuthenticationTls.TYPE_TLS, value = KafkaClientAuthenticationTls.class),
    @JsonSubTypes.Type(name = KafkaClientAuthenticationScramSha256.TYPE_SCRAM_SHA_256, value = KafkaClientAuthenticationScramSha256.class),
    @JsonSubTypes.Type(name = KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512, value = KafkaClientAuthenticationScramSha512.class),
    @JsonSubTypes.Type(name = KafkaClientAuthenticationPlain.TYPE_PLAIN, value = KafkaClientAuthenticationPlain.class),
    @JsonSubTypes.Type(name = KafkaClientAuthenticationOAuth.TYPE_OAUTH, value = KafkaClientAuthenticationOAuth.class),
    @JsonSubTypes.Type(name = KafkaClientAuthenticationCustom.TYPE_CUSTOM, value = KafkaClientAuthenticationCustom.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class KafkaClientAuthentication implements UnknownPropertyPreserving {
    private Map<String, Object> additionalProperties;

    @Description("Specifies the authentication type. " +
            "Supported types are `tls`, `scram-sha-256`, `scram-sha-512`, `plain`, 'oauth', and `custom`. " +
            "`tls` uses TLS client authentication and is supported only over TLS connections. " +
            "`scram-sha-256` and `scram-sha-512` use SASL SCRAM-SHA-256 and SASL SCRAM-SHA-512 authentication, respectively. " +
            "`plain` uses SASL PLAIN authentication. " +
            "`oauth` uses SASL OAUTHBEARER authentication. " +
            "`custom` allows you to configure a custom authentication mechanism. " +
            "As of Strimzi 0.49.0, `oauth` type is deprecated and will be removed in the `v1` API version. " +
            "Please use `custom` type instead.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
