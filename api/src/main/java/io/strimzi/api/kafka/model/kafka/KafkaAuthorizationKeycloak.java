/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Configures Keycloak authorization on the brokers
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "clientId", "tokenEndpointUri", "tlsTrustedCertificates", "disableTlsHostnameVerification",
    "delegateToKafkaAcls", "grantsRefreshPeriodSeconds", "grantsRefreshPoolSize", "grantsMaxIdleTimeSeconds",
    "grantsGcPeriodSeconds", "grantsAlwaysLatest", "superUsers", "connectTimeoutSeconds", "readTimeoutSeconds",
    "httpRetries", "enableMetrics", "includeAcceptHeader"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Deprecated
@DeprecatedType(replacedWithType = KafkaAuthorizationCustom.class)
@PresentInVersions("v1beta2")
public class KafkaAuthorizationKeycloak extends KafkaAuthorization {
    public static final String TYPE_KEYCLOAK = "keycloak";

    public static final String AUTHORIZER_CLASS_NAME = "io.strimzi.kafka.oauth.server.authorizer.KeycloakAuthorizer";

    private String clientId;
    private String tokenEndpointUri;
    private List<CertSecretSource> tlsTrustedCertificates;
    private boolean disableTlsHostnameVerification = false;
    private boolean delegateToKafkaAcls = false;
    private Integer grantsRefreshPeriodSeconds;
    private Integer grantsRefreshPoolSize;
    private Integer grantsMaxIdleTimeSeconds;
    private Integer grantsGcPeriodSeconds;
    private boolean grantsAlwaysLatest = false;
    private Integer connectTimeoutSeconds;
    private Integer readTimeoutSeconds;
    private Integer httpRetries;
    private List<String> superUsers;
    private boolean enableMetrics = false;
    private boolean includeAcceptHeader = true;

    @Description("Must be `" + TYPE_KEYCLOAK + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_KEYCLOAK;
    }

    /**
     * When delegation to Kafka Simple Authorizer is enabled, the Kafka Admin API can be used to manage the Kafka ACLs.
     * If it is disabled, using the Admin API is not possible.
     *
     * @return True when delegation to Kafka Simple Authorizer is enabled and the Kafka Admin API can be used to manage Simple ACLs.
     */
    public boolean supportsAdminApi()   {
        return delegateToKafkaAcls;
    }

    @Description("OAuth Client ID which the Kafka client can use to authenticate against the OAuth server and use the token endpoint URI.")
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Description("Authorization server token endpoint URI.")
    public String getTokenEndpointUri() {
        return tokenEndpointUri;
    }

    public void setTokenEndpointUri(String tokenEndpointUri) {
        this.tokenEndpointUri = tokenEndpointUri;
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

    @Description("Whether authorization decision should be delegated to the 'Simple' authorizer if DENIED by Keycloak Authorization Services policies." +
            " Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isDelegateToKafkaAcls() {
        return delegateToKafkaAcls;
    }

    public void setDelegateToKafkaAcls(boolean delegateToKafkaAcls) {
        this.delegateToKafkaAcls = delegateToKafkaAcls;
    }

    @Description("The time between two consecutive grants refresh runs in seconds. The default value is 60.")
    @Minimum(0)
    @JsonProperty(defaultValue = "60")
    public Integer getGrantsRefreshPeriodSeconds() {
        return grantsRefreshPeriodSeconds;
    }

    public void setGrantsRefreshPeriodSeconds(Integer grantsRefreshPeriodSeconds) {
        this.grantsRefreshPeriodSeconds = grantsRefreshPeriodSeconds;
    }

    @Description("The number of threads to use to refresh grants for active sessions. The more threads, the more parallelism," +
            " so the sooner the job completes. However, using more threads places a heavier load on the authorization server. The default value is 5.")
    @Minimum(1)
    @JsonProperty(defaultValue = "5")
    public Integer getGrantsRefreshPoolSize() {
        return grantsRefreshPoolSize;
    }

    public void setGrantsRefreshPoolSize(Integer grantsRefreshPoolSize) {
        this.grantsRefreshPoolSize = grantsRefreshPoolSize;
    }

    @Description("The time, in seconds, after which an idle grant can be evicted from the cache. The default value is 300.")
    @Minimum(1)
    @JsonProperty(defaultValue = "300")
    public Integer getGrantsMaxIdleTimeSeconds() {
        return grantsMaxIdleTimeSeconds;
    }

    public void setGrantsMaxIdleTimeSeconds(Integer grantsMaxIdleTimeSeconds) {
        this.grantsMaxIdleTimeSeconds = grantsMaxIdleTimeSeconds;
    }

    @Description("The time, in seconds, between consecutive runs of a job that cleans stale grants from the cache. The default value is 300.")
    @Minimum(1)
    @JsonProperty(defaultValue = "300")
    public Integer getGrantsGcPeriodSeconds() {
        return grantsGcPeriodSeconds;
    }

    public void setGrantsGcPeriodSeconds(Integer grantsGcPeriodSeconds) {
        this.grantsGcPeriodSeconds = grantsGcPeriodSeconds;
    }

    @Description("Controls whether the latest grants are fetched for a new session. When enabled, grants are retrieved from Keycloak and cached for the user. The default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isGrantsAlwaysLatest() {
        return grantsAlwaysLatest;
    }

    public void setGrantsAlwaysLatest(boolean grantsAlwaysLatest) {
        this.grantsAlwaysLatest = grantsAlwaysLatest;
    }

    @Description("List of super users. Should contain list of user principals which should get unlimited access rights.")
    @Example("- CN=my-user\n" +
            "- CN=my-other-user")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getSuperUsers() {
        return superUsers;
    }

    public void setSuperUsers(List<String> superUsers) {
        this.superUsers = superUsers;
    }

    @Description("The connect timeout in seconds when connecting to authorization server. If not set, the effective connect timeout is 60 seconds.")
    @Minimum(1)
    @JsonProperty(defaultValue = "60")
    public Integer getConnectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    public void setConnectTimeoutSeconds(Integer connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    @Description("The read timeout in seconds when connecting to authorization server. If not set, the effective read timeout is 60 seconds.")
    @Minimum(1)
    @JsonProperty(defaultValue = "60")
    public Integer getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public void setReadTimeoutSeconds(Integer readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    @Description("The maximum number of retries to attempt if an initial HTTP request fails. If not set, the default is to not attempt any retries.")
    @Minimum(0)
    @JsonProperty(defaultValue = "0")
    public Integer getHttpRetries() {
        return httpRetries;
    }

    public void setHttpRetries(Integer httpRetries) {
        this.httpRetries = httpRetries;
    }

    @Description("Enable or disable OAuth metrics. The default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
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
