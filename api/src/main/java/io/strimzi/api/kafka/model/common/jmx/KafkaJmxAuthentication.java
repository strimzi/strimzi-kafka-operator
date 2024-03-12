/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.jmx;

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

/*
 * Configure the authentication protocol on the Kafka Broker's JMX port.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@JsonSubTypes({
    @JsonSubTypes.Type(name = KafkaJmxAuthenticationPassword.TYPE_PASSWORD, value = KafkaJmxAuthenticationPassword.class)
})
@ToString
public abstract class KafkaJmxAuthentication implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Authentication type. " +
            "Currently the only supported types are `password`." +
            "`password` type creates a username and protected port with no TLS.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
