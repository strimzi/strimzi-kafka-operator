/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Configures a listener to use OAuth authentication.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "clientId", "clientSecret", "validIssuerUri", "checkIssuer", "checkAudience",
    "jwksEndpointUri", "jwksRefreshSeconds", "jwksMinRefreshPauseSeconds", "jwksExpirySeconds", "jwksIgnoreKeyUse",
    "introspectionEndpointUri", "userNameClaim", "fallbackUserNameClaim", "fallbackUserNamePrefix",
    "groupsClaim", "groupsClaimDelimiter", "userInfoEndpointUri", "checkAccessTokenType", "validTokenType",
    "accessTokenIsJwt", "tlsTrustedCertificates", "disableTlsHostnameVerification", "enableECDSA",
    "maxSecondsWithoutReauthentication", "enablePlain", "tokenEndpointUri", "enableOauthBearer", "customClaimCheck",
    "connectTimeoutSeconds", "readTimeoutSeconds", "httpRetries", "httpRetryPauseMs", "clientScope", "clientAudience",
    "enableMetrics", "failFast", "includeAcceptHeader"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaListenerAuthenticationOAuth extends KafkaListenerAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OAUTH = "oauth";
    public static final int DEFAULT_JWKS_EXPIRY_SECONDS = 360;
    public static final int DEFAULT_JWKS_REFRESH_SECONDS = 300;

    public static final String PRINCIPAL_BUILDER_CLASS_NAME = "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder";

    private String clientId;
    private GenericSecretSource clientSecret;
    private String validIssuerUri;
    private boolean checkIssuer = true;
    private boolean checkAudience = false;
    private String jwksEndpointUri;
    private Integer jwksRefreshSeconds;
    private Integer jwksMinRefreshPauseSeconds;
    private Integer jwksExpirySeconds;
    private boolean jwksIgnoreKeyUse = false;
    private String introspectionEndpointUri;
    private String userNameClaim;
    private String fallbackUserNameClaim;
    private String fallbackUserNamePrefix;
    private String groupsClaim;
    private String groupsClaimDelimiter;
    private String userInfoEndpointUri;
    private boolean checkAccessTokenType = true;
    private String validTokenType;
    private boolean accessTokenIsJwt = true;
    private List<CertSecretSource> tlsTrustedCertificates;
    private boolean disableTlsHostnameVerification = false;
    private Boolean enableECDSA;
    private Integer maxSecondsWithoutReauthentication;
    private boolean enablePlain = false;
    private String tokenEndpointUri;
    private boolean enableOauthBearer = true;
    private String customClaimCheck;
    private Integer connectTimeoutSeconds;
    private Integer readTimeoutSeconds;
    private Integer httpRetries;
    private Integer httpRetryPauseMs;
    private String clientScope = null;
    private String clientAudience = null;
    private boolean enableMetrics = false;
    private boolean failFast = true;
    private boolean includeAcceptHeader = true;

    @Description("Must be `" + TYPE_OAUTH + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
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

    @Description("Enable or disable audience checking. Audience checks identify the recipients of tokens. " +
            "If audience checking is enabled, the OAuth Client ID also has to be configured using the `clientId` property. " +
            "The Kafka broker will reject tokens that do not have its `clientId` in their `aud` (audience) claim." +
            "Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isCheckAudience() {
        return checkAudience;
    }

    public void setCheckAudience(boolean checkAudience) {
        this.checkAudience = checkAudience;
    }

    @Description("JsonPath filter query to be applied to the JWT token or to the response of the introspection endpoint " +
            "for additional token validation. Not set by default.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getCustomClaimCheck() {
        return customClaimCheck;
    }

    public void setCustomClaimCheck(String customClaimCheck) {
        this.customClaimCheck = customClaimCheck;
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

    @Description("The maximum number of retries to attempt if an initial HTTP request fails. If not set, the default is to not attempt any retries.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getHttpRetries() {
        return httpRetries;
    }

    public void setHttpRetries(Integer httpRetries) {
        this.httpRetries = httpRetries;
    }

    @Description("The pause to take before retrying a failed HTTP request. If not set, the default is to not pause at all but to immediately repeat a request.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getHttpRetryPauseMs() {
        return httpRetryPauseMs;
    }

    public void setHttpRetryPauseMs(Integer httpRetryPauseMs) {
        this.httpRetryPauseMs = httpRetryPauseMs;
    }

    @Description("The scope to use when making requests to the authorization server's token endpoint. Used for inter-broker authentication and for configuring OAuth 2.0 over PLAIN using the `clientId` and `secret` method.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getClientScope() {
        return clientScope;
    }

    public void setClientScope(String scope) {
        this.clientScope = scope;
    }

    @Description("The audience to use when making requests to the authorization server's token endpoint. Used for inter-broker authentication and for configuring OAuth 2.0 over PLAIN using the `clientId` and `secret` method.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getClientAudience() {
        return clientAudience;
    }

    public void setClientAudience(String audience) {
        this.clientAudience = audience;
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
    @JsonProperty(defaultValue = "300")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getJwksRefreshSeconds() {
        return jwksRefreshSeconds;
    }

    public void setJwksRefreshSeconds(Integer jwksRefreshSeconds) {
        this.jwksRefreshSeconds = jwksRefreshSeconds;
    }

    @Description("The minimum pause between two consecutive refreshes. When an unknown signing key is encountered the refresh is scheduled immediately, but will always wait for this minimum pause. Defaults to 1 second.")
    @Minimum(0)
    @JsonProperty(defaultValue = "1")
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
    @JsonProperty(defaultValue = "360")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getJwksExpirySeconds() {
        return jwksExpirySeconds;
    }

    public void setJwksExpirySeconds(Integer jwksExpirySeconds) {
        this.jwksExpirySeconds = jwksExpirySeconds;
    }

    @Description("Flag to ignore the 'use' attribute of `key` declarations in a JWKS endpoint response. Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean getJwksIgnoreKeyUse() {
        return jwksIgnoreKeyUse;
    }

    public void setJwksIgnoreKeyUse(boolean jwksIgnoreKeyUse) {
        this.jwksIgnoreKeyUse = jwksIgnoreKeyUse;
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

    @Description("JsonPath query used to extract groups for the user during authentication. Extracted groups can be used by a custom authorizer. By default no groups are extracted.")
    public String getGroupsClaim() {
        return groupsClaim;
    }

    public void setGroupsClaim(String groupsClaim) {
        this.groupsClaim = groupsClaim;
    }

    @Description("A delimiter used to parse groups when they are extracted as a single String value rather than a JSON array. Default value is ',' (comma).")
    public String getGroupsClaimDelimiter() {
        return groupsClaimDelimiter;
    }

    public void setGroupsClaimDelimiter(String groupsClaimDelimiter) {
        this.groupsClaimDelimiter = groupsClaimDelimiter;
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

    @DeprecatedProperty
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    @Description("Enable or disable ECDSA support by installing BouncyCastle crypto provider. " +
            "ECDSA support is always enabled. The BouncyCastle libraries are no longer packaged with Strimzi. Value is ignored.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public Boolean getEnableECDSA() {
        return enableECDSA;
    }

    public void setEnableECDSA(Boolean enableECDSA) {
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
            "Not set by default - the authenticated session does not expire when the access token expires. This option only applies to SASL_OAUTHBEARER authentication mechanism (when `enableOauthBearer` is `true`).")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getMaxSecondsWithoutReauthentication() {
        return maxSecondsWithoutReauthentication;
    }

    public void setMaxSecondsWithoutReauthentication(Integer maxSecondsWithoutReauthentication) {
        this.maxSecondsWithoutReauthentication = maxSecondsWithoutReauthentication;
    }

    @Description("Enable or disable OAuth authentication over SASL_OAUTHBEARER. " +
            "Default value is `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isEnableOauthBearer() {
        return enableOauthBearer;
    }

    public void setEnableOauthBearer(boolean enableOauthBearer) {
        this.enableOauthBearer = enableOauthBearer;
    }

    @Description("Enable or disable OAuth authentication over SASL_PLAIN. There is no re-authentication support when this mechanism is used. " +
            "Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isEnablePlain() {
        return enablePlain;
    }

    public void setEnablePlain(boolean enablePlain) {
        this.enablePlain = enablePlain;
    }

    @Description("URI of the Token Endpoint to use with SASL_PLAIN mechanism when the client authenticates with `clientId` and a `secret`. " +
            "If set, the client can authenticate over SASL_PLAIN by either setting `username` to `clientId`, and setting `password` to client `secret`, " +
            "or by setting `username` to account username, and `password` to access token prefixed with `$accessToken:`. If this option is not set, " +
            "the `password` is always interpreted as an access token (without a prefix), and `username` as the account username (a so called 'no-client-credentials' mode).")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTokenEndpointUri() {
        return tokenEndpointUri;
    }

    public void setTokenEndpointUri(String tokenEndpointUri) {
        this.tokenEndpointUri = tokenEndpointUri;
    }

    @Description("Enable or disable OAuth metrics. Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    @Description("Enable or disable termination of Kafka broker processes due to potentially recoverable runtime errors during startup. Default value is `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean getFailFast() {
        return failFast;
    }

    public void setFailFast(boolean failFast) {
        this.failFast = failFast;
    }

    @Description("Whether the Accept header should be set in requests to the authorization servers. The default value is `true`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isIncludeAcceptHeader() {
        return includeAcceptHeader;
    }

    public void setIncludeAcceptHeader(boolean includeAcceptHeader) {
        this.includeAcceptHeader = includeAcceptHeader;
    }
}
