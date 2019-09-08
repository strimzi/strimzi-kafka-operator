/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;
import lombok.EqualsAndHashCode;

/**
 * Configures a listener to use OAuth authentication.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaListenerAuthenticationOAuth extends KafkaListenerAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OAUTH = "oauth";

    private String issuerUri;
    private String jwksEndpointUri;
    private int jwksRefreshSeconds;
    private int jwksExpirySeconds;
    private String introspectionEndpointUri;
    private String userNameClaim;

    @Description("Must be `" + TYPE_OAUTH + "`")
    @Override
    public String getType() {
        return TYPE_OAUTH;
    }

    @Description("URI of the token issuer used for authentication.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getIssuerUri() {
        return issuerUri;
    }

    public void setIssuerUri(String issuerUri) {
        this.issuerUri = issuerUri;
    }

    @Description("URI of the JWKS certificate endpoint, which can be used for local JWT validation.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getJwksEndpointUri() {
        return jwksEndpointUri;
    }

    public void setJwksEndpointUri(String jwksEndpointUri) {
        this.jwksEndpointUri = jwksEndpointUri;
    }

    @Description("Configures how often are the JWKS certificates refreshed. " +
            "Defaults to 300 seconds.")
    @Minimum(1)
    @DefaultValue("300")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public int getJwksRefreshSeconds() {
        return jwksRefreshSeconds;
    }

    public void setJwksRefreshSeconds(int jwksRefreshSeconds) {
        this.jwksRefreshSeconds = jwksRefreshSeconds;
    }

    @Description("Configures how often are the JWKS certificates considered valid. " +
            "Defaults to 360 seconds.")
    @Minimum(1)
    @DefaultValue("360")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public int getJwksExpirySeconds() {
        return jwksExpirySeconds;
    }

    public void setJwksExpirySeconds(int jwksExpirySeconds) {
        this.jwksExpirySeconds = jwksExpirySeconds;
    }

    @Description("URI of the token introspection endpoint which can be used to validate opaque non-JWT tokens.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getIntrospectionEndpointUri() {
        return introspectionEndpointUri;
    }

    public void setIntrospectionEndpointUri(String introspectionEndpointUri) {
        this.introspectionEndpointUri = introspectionEndpointUri;
    }

    @Description("Name of the claim from the authentication token which will be used as the user principal. " +
            "Defaults to `sub`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getUserNameClaim() {
        return userNameClaim;
    }

    public void setUserNameClaim(String userNameClaim) {
        this.userNameClaim = userNameClaim;
    }
}
