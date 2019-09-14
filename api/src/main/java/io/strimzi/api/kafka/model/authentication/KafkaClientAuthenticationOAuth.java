/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the Kafka client authentication in client based components
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaClientAuthenticationOAuth extends KafkaClientAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OAUTH = "oauth";

    private String clientId;
    private String tokenEndpointUri;
    private GenericSecretSource clientSecret;
    private GenericSecretSource accessToken;
    private GenericSecretSource refreshToken;
    private List<CertSecretSource> tlsTrustedCertificates;
    private boolean disableTlsHostnameVerification = false;

    @Description("Must be `" + TYPE_OAUTH + "`")
    @Override
    public String getType() {
        return TYPE_OAUTH;
    }

    @Description("OAuth Client ID which the Kafka Bridge can use to authenticate against the OAuth server and use the token endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Description("Authorization server token endpoint URI.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getTokenEndpointUri() {
        return tokenEndpointUri;
    }

    public void setTokenEndpointUri(String tokenEndpointUri) {
        this.tokenEndpointUri = tokenEndpointUri;
    }

    @Description("Link to Kubernetes Secret containing the OAuth client secret which the Kafka Bridge can use to authenticate against the OAuth server and use the token endpoint URI.")
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
}
