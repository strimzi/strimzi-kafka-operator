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
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

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
public class HashLoginServiceApiUsers extends ApiUsers {
    public static final String TYPE_HASH_LOGIN_SERVICE = "hashLoginService";

    private PasswordSource valueFrom;

    @Description("Must be " + "`" + TYPE_HASH_LOGIN_SERVICE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_HASH_LOGIN_SERVICE;
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
}
