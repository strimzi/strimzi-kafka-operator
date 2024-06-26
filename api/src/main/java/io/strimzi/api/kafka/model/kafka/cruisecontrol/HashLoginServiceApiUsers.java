/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.PasswordSource;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

/**
 * Cruise Control's API users config
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "valueFrom"})
@EqualsAndHashCode
public class HashLoginServiceApiUsers implements UnknownPropertyPreserving {
    public static final String TYPE_HASH_LOGIN_SERVICE = "hashLoginService";

    private PasswordSource valueFrom;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Type of the Cruise Control API Users configuration. " +
            "Supported values are: " + "`" + TYPE_HASH_LOGIN_SERVICE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(required = true)
    public String getType() {
        return TYPE_HASH_LOGIN_SERVICE;
    }

    @Description("Secret from which the custom Cruise Control API authentication credentials should be read. ")
    @JsonProperty(required = true)
    public PasswordSource getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(PasswordSource valueFrom) {
        this.valueFrom = valueFrom;
    }

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
