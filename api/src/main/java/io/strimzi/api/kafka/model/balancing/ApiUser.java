/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.balancing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.Password;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Cruise Control API user settings.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "password", "role"})
@EqualsAndHashCode
public class ApiUser implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    // Regular expression for matching any lowercase alphanumeric string
    private final static String NAME_REGEX = "[a-z0-9]+$";
    private static final String FORBIDDEN_NAMES = "admin, healthcheck";

    private String name;
    private Password password;
    private String role;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Cruise Control REST API user. " +
            "The following names are reserved for Strimzi and cannot be used: " +  FORBIDDEN_NAMES)
    @JsonProperty(required = true)
    @Pattern(NAME_REGEX)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Specify the password for the user Cruise Control REST API user.")
    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Pattern("^(ADMIN)|(USER)|(VIEWER)$")
    @Description("Cruise Control REST API user role." +
            "Valid API user roles are VIEWER, USER, and ADMIN" +
            "For more information on valid API user roles see https://github.com/linkedin/cruise-control/wiki/Security#authorization")
    @JsonProperty(required = true)
    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
