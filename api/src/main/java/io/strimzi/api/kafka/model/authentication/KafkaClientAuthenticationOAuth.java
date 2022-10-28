/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the Kafka client authentication using SASl OAUTHBEARER mechanism in client based components
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaClientAuthenticationOAuth extends KafkaClientAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OAUTH = "oauth";

    private String clientId;
    private String username;
    private String scope;
    private String audience;
    private String tokenEndpointUri;
    private Integer connectTimeoutSeconds;
    private Integer readTimeoutSeconds;
    private GenericSecretSource clientSecret;
    private PasswordSecretSource passwordSecret;
    private GenericSecretSource accessToken;
    private GenericSecretSource refreshToken;
    private List<CertSecretSource> tlsTrustedCertificates;
    private boolean disableTlsHostnameVerification = false;
    private int maxTokenExpirySeconds = 0;
    private boolean accessTokenIsJwt = true;
    private boolean enableMetrics = false;

    @Description("Must be `" + TYPE_OAUTH + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_OAUTH;
    }

    @Description("OAuth Client ID which the Kafka client can use to authenticate against the OAuth server and use the token endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Description("OAuth scope to use when authenticating against the authorization server. Some authorization servers require this to be set. "
            + "The possible values depend on how authorization server is configured. By default `scope` is not specified when doing the token endpoint request.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Description("OAuth audience to use when authenticating against the authorization server. Some authorization servers require the audience to be explicitly set. "
            + "The possible values depend on how the authorization server is configured. By default, `audience` is not specified when performing the token endpoint request.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getAudience() {
        return audience;
    }

    public void setAudience(String audience) {
        this.audience = audience;
    }

    @Description("Authorization server token endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getTokenEndpointUri() {
        return tokenEndpointUri;
    }

    public void setTokenEndpointUri(String tokenEndpointUri) {
        this.tokenEndpointUri = tokenEndpointUri;
    }

    @Description("The connect timeout in seconds when connecting to authorization server. If not set, the effective connect timeout is 60 seconds.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getConnectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    public void setConnectTimeoutSeconds(Integer connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    @Description("The read timeout in seconds when connecting to authorization server. If not set, the effective read timeout is 60 seconds.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public void setReadTimeoutSeconds(Integer readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    @Description("Link to Kubernetes Secret containing the OAuth client secret which the Kafka client can use to authenticate "
            + "against the OAuth server and use the token endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public GenericSecretSource getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(GenericSecretSource clientSecret) {
        this.clientSecret = clientSecret;
    }

    @Description("Link to Kubernetes Secret containing the access token which was obtained from the authorization server.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public GenericSecretSource getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(GenericSecretSource accessToken) {
        this.accessToken = accessToken;
    }

    @Description("Link to Kubernetes Secret containing the refresh token which can be used to obtain access token from the authorization server.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public GenericSecretSource getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(GenericSecretSource refreshToken) {
        this.refreshToken = refreshToken;
    }

    @Description("Trusted certificates for TLS connection to the OAuth server.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<CertSecretSource> getTlsTrustedCertificates() {
        return tlsTrustedCertificates;
    }

    public void setTlsTrustedCertificates(List<CertSecretSource> tlsTrustedCertificates) {
        this.tlsTrustedCertificates = tlsTrustedCertificates;
    }

    @Description("Enable or disable TLS hostname verification. " +
            "Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isDisableTlsHostnameVerification() {
        return disableTlsHostnameVerification;
    }

    public void setDisableTlsHostnameVerification(boolean disableTlsHostnameVerification) {
        this.disableTlsHostnameVerification = disableTlsHostnameVerification;
    }

    @Description("Set or limit time-to-live of the access tokens to the specified number of seconds. " +
            "This should be set if the authorization server returns opaque tokens.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getMaxTokenExpirySeconds() {
        return maxTokenExpirySeconds;
    }

    public void setMaxTokenExpirySeconds(int maxTokenExpirySeconds) {
        this.maxTokenExpirySeconds = maxTokenExpirySeconds;
    }

    @Description("Configure whether access token should be treated as JWT. This should be set to `false` if the authorization " +
            "server returns opaque tokens. Defaults to `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isAccessTokenIsJwt() {
        return accessTokenIsJwt;
    }

    public void setAccessTokenIsJwt(boolean accessTokenIsJwt) {
        this.accessTokenIsJwt = accessTokenIsJwt;
    }

    @Description("Enable or disable OAuth metrics. Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    @Description("Reference to the `Secret` which holds the password.")
    public PasswordSecretSource getPasswordSecret() {
        return passwordSecret;
    }

    public void setPasswordSecret(PasswordSecretSource passwordSecret) {
        this.passwordSecret = passwordSecret;
    }

    @Description("Username used for the authentication.")
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
