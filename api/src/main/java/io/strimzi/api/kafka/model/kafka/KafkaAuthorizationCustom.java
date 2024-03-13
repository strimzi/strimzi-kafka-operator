/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
@JsonPropertyOrder({"type", "authorizerClass", "superUsers", "supportsAdminApi"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaAuthorizationCustom extends KafkaAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_CUSTOM = "custom";

    private String authorizerClass;
    private List<String> superUsers;
    private boolean supportsAdminApi = false;

    @Description("Must be `" + TYPE_CUSTOM + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_CUSTOM;
    }

    /**
     * Custom Authorizer might or might not support the APIs for managing ACLs.
     *
     * @return Returns true if the custom authorizer supports APIs for ACL management. False otherwise.
     */
    public boolean supportsAdminApi()   {
        return supportsAdminApi;
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

    @Description("Indicates whether the custom authorizer supports the APIs for managing ACLs using the Kafka Admin API. " +
            "Defaults to `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isSupportsAdminApi() {
        return supportsAdminApi;
    }

    public void setSupportsAdminApi(boolean supportsAdminApi) {
        this.supportsAdminApi = supportsAdminApi;
    }
}
