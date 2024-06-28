/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.PasswordSource;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Cruise Control's API users config
 */
@JsonSubTypes({
    @JsonSubTypes.Type(name = HashLoginServiceApiUsers.TYPE_HASH_LOGIN_SERVICE, value = HashLoginServiceApiUsers.class),
})
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "valueFrom"})
@EqualsAndHashCode
public abstract class ApiUsers implements UnknownPropertyPreserving {
    private String type;
    private PasswordSource valueFrom;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Type of the Cruise Control API users configuration. ")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(required = true)
    public abstract String getType();

    public void setType(String type) {
        this.type = type;
    }

    @Description("Secret from which the custom Cruise Control API authentication credentials are read. ")
    @JsonInclude(JsonInclude.Include.NON_NULL)
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

    @Description("Regex pattern used to parse Cruise Control API Users configuration")
    @JsonAnyGetter
    public abstract Pattern getPattern();
}
