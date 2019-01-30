/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "authentication" })
@EqualsAndHashCode
public class KafkaUserSpec  implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private KafkaUserAuthentication authentication;
    private KafkaUserAuthorization authorization;
    private Map<String, Object> additionalProperties;

    @Description("Authentication mechanism enabled for this Kafka user.")
    @JsonProperty(required = true)
    public KafkaUserAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("Authorization rules for this Kafka user.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaUserAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(KafkaUserAuthorization authorization) {
        this.authorization = authorization;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
