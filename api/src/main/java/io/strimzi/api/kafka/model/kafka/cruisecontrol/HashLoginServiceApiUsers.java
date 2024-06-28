/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.regex.Pattern;

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

    // Regex to match an entry in Jetty's HashLoginService's file format: username: password, rolename
    private static final Pattern HASH_LOGIN_SERVICE_PATTERN = Pattern.compile("^[\\w-]+\\s*:\\s*\\w+\\s*,\\s*\\w+\\s*$");

    @Description("Type of the Cruise Control API users configuration. " +
            "Supported format is: " + "`" + TYPE_HASH_LOGIN_SERVICE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(required = true)
    public String getType() {
        return TYPE_HASH_LOGIN_SERVICE;
    }

    @Description("Regex pattern used to parse Cruise Control API Users configuration")
    public Pattern getPattern() {
        return HASH_LOGIN_SERVICE_PATTERN;
    }
}
