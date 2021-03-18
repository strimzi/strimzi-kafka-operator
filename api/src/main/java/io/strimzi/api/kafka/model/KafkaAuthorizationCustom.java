/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the broker for custom authorization module
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "authorizerClass", "superUsers"})
@EqualsAndHashCode
public class KafkaAuthorizationCustom extends KafkaAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_CUSTOM = "custom";

    private String authorizerClass;
    private List<String> superUsers;

    @Description("Must be `" + TYPE_CUSTOM + "`")
    @Override
    public String getType() {
        return TYPE_CUSTOM;
    }

    @Description("List of super users, which are user principals with unlimited access rights.")
    @Example("- CN=my-user\n" +
             "- CN=my-other-user")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getSuperUsers() {
        return superUsers;
    }

    public void setSuperUsers(List<String> superUsers) {
        this.superUsers = superUsers;
    }

    @Description("Authorization implementation class, which must be available in classpath")
    public String getAuthorizerClass() {
        return authorizerClass;
    }

    public void setAuthorizerClass(String clazz) {
        this.authorizerClass = clazz;
    }
}
