/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures a listener to use OAuth authentication.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaListenerAuthenticationOAuth extends KafkaListenerAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OAUTH = "oauth";
    public static final int DEFAULT_JWKS_EXPIRY_SECONDS = 360;
    public static final int DEFAULT_JWKS_REFRESH_SECONDS = 300;

    private String clientId;
    private GenericSecretSource clientSecret;
    private String validIssuerUri;
    private boolean checkIssuer = true;
    private String jwksEndpointUri;
    private Integer jwksRefreshSeconds;
    private Integer jwksMinRefreshPauseSeconds;
    private Integer jwksExpirySeconds;
    private String introspectionEndpointUri;
    private String userNameClaim;
    private String fallbackUserNameClaim;
    private String fallbackUserNamePrefix;
    private String userInfoEndpointUri;
    private boolean checkAccessTokenType = true;
    private String validTokenType;
    private boolean accessTokenIsJwt = true;
    private List<CertSecretSource> tlsTrustedCertificates;
    private boolean disableTlsHostnameVerification = false;
    private boolean enableECDSA = false;
    private Integer maxSecondsWithoutReauthentication;

    @Description("Must be `" + TYPE_OAUTH + "`")
    @Override
    public String getType() {
        return TYPE_OAUTH;
    }

    @Description("OAuth Client ID which the Kafka broker can use to authenticate against the authorization server and use the introspect endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Description("Link to Kubernetes Secret containing the OAuth client secret which the Kafka broker can use to authenticate against the authorization server and use the introspect endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public GenericSecretSource getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(GenericSecretSource clientSecret) {
        this.clientSecret = clientSecret;
    }

    @Description("URI of the token issuer used for authentication.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getValidIssuerUri() {
        return validIssuerUri;
    }

    public void setValidIssuerUri(String validIssuerUri) {
        this.validIssuerUri = validIssuerUri;
    }

    @Description("Enable or disable issuer checking. By default issuer is checked using the value configured by `validIssuerUri`. " +
            "Default value is `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isCheckIssuer() {
        return checkIssuer;
    }

    public void setCheckIssuer(boolean checkIssuer) {
        this.checkIssuer = checkIssuer;
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
            "The refresh interval has to be at least 60 seconds shorter then the expiry interval specified in `jwksExpirySeconds`. " +
            "Defaults to 300 seconds.")
    @Minimum(1)
    @DefaultValue("300")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getJwksRefreshSeconds() {
        return jwksRefreshSeconds;
    }

    public void setJwksRefreshSeconds(Integer jwksRefreshSeconds) {
        this.jwksRefreshSeconds = jwksRefreshSeconds;
    }

    @Description("The minimum pause between two consecutive refreshes. When an unknown signing key is encountered the refresh is scheduled immediately, but will always wait for this minimum pause. Defaults to 1 second.")
    @Minimum(0)
    @DefaultValue("1")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getJwksMinRefreshPauseSeconds() {
        return jwksMinRefreshPauseSeconds;
    }

    public void setJwksMinRefreshPauseSeconds(Integer jwksMinRefreshPauseSeconds) {
        this.jwksMinRefreshPauseSeconds = jwksMinRefreshPauseSeconds;
    }

    @Description("Configures how often are the JWKS certificates considered valid. " +
            "The expiry interval has to be at least 60 seconds longer then the refresh interval specified in `jwksRefreshSeconds`. " +
            "Defaults to 360 seconds.")
    @Minimum(1)
    @DefaultValue("360")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getJwksExpirySeconds() {
        return jwksExpirySeconds;
    }

    public void setJwksExpirySeconds(Integer jwksExpirySeconds) {
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

    @Description("Name of the claim from the JWT authentication token, Introspection Endpoint response or User Info Endpoint response " +
            "which will be used to extract the user id. Defaults to `sub`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getUserNameClaim() {
        return userNameClaim;
    }

    public void setUserNameClaim(String userNameClaim) {
        this.userNameClaim = userNameClaim;
    }

    @Description("The fallback username claim to be used for the user id if the claim specified by `userNameClaim` is not present. " +
            "This is useful when `client_credentials` authentication only results in the client id being provided in another claim. " +
            "It only takes effect if `userNameClaim` is set.")
    public String getFallbackUserNameClaim() {
        return fallbackUserNameClaim;
    }

    public void setFallbackUserNameClaim(String fallbackUserNameClaim) {
        this.fallbackUserNameClaim = fallbackUserNameClaim;
    }

    @Description("The prefix to use with the value of `fallbackUserNameClaim` to construct the user id. " +
            "This only takes effect if `fallbackUserNameClaim` is true, and the value is present for the claim. " +
            "Mapping usernames and client ids into the same user id space is useful in preventing name collisions.")
    public String getFallbackUserNamePrefix() {
        return fallbackUserNamePrefix;
    }

    public void setFallbackUserNamePrefix(String fallbackUserNamePrefix) {
        this.fallbackUserNamePrefix = fallbackUserNamePrefix;
    }

    @Description("Configure whether the access token type check is performed or not. This should be set to `false` " +
            "if the authorization server does not include 'typ' claim in JWT token. Defaults to `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isCheckAccessTokenType() {
        return checkAccessTokenType;
    }

    public void setCheckAccessTokenType(boolean checkAccessTokenType) {
        this.checkAccessTokenType = checkAccessTokenType;
    }

    @Description("Valid value for the `token_type` attribute returned by the Introspection Endpoint. No default value, and not checked by default.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getValidTokenType() {
        return validTokenType;
    }

    public void setValidTokenType(String validTokenType) {
        this.validTokenType = validTokenType;
    }

    @Description("Configure whether the access token is treated as JWT. This must be set to `false` if " +
            "the authorization server returns opaque tokens. Defaults to `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isAccessTokenIsJwt() {
        return accessTokenIsJwt;
    }

    public void setAccessTokenIsJwt(boolean accessTokenIsJwt) {
        this.accessTokenIsJwt = accessTokenIsJwt;
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

    @Description("Enable or disable ECDSA support by installing BouncyCastle crypto provider. " +
            "Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isEnableECDSA() {
        return enableECDSA;
    }

    public void setEnableECDSA(boolean enableECDSA) {
        this.enableECDSA = enableECDSA;
    }

    @Description("URI of the User Info Endpoint to use as a fallback to obtaining the user id when the Introspection Endpoint " +
            "does not return information that can be used for the user id. ")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getUserInfoEndpointUri() {
        return userInfoEndpointUri;
    }

    public void setUserInfoEndpointUri(String userInfoEndpointUri) {
        this.userInfoEndpointUri = userInfoEndpointUri;
    }

    @Description("Maximum number of seconds the authenticated session remains valid without re-authentication. This enables Apache Kafka re-authentication feature, and causes sessions to expire when the access token expires. " +
            "If the access token expires before max time or if max time is reached, the client has to re-authenticate, otherwise the server will drop the connection. " +
            "Not set by default - the authenticated session does not expire when the access token expires.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getMaxSecondsWithoutReauthentication() {
        return maxSecondsWithoutReauthentication;
    }

    public void setMaxSecondsWithoutReauthentication(Integer maxSecondsWithoutReauthentication) {
        this.maxSecondsWithoutReauthentication = maxSecondsWithoutReauthentication;
    }
}
