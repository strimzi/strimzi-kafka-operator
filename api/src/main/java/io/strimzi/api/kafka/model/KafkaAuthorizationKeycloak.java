/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures Keycloak authorization on the brokers
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "clientId", "tokenEndpointUri",
                    "tlsTrustedCertificates", "disableTlsHostnameVerification",
                    "delegateToKafkaAcls", "superUsers"})
@EqualsAndHashCode
public class KafkaAuthorizationKeycloak extends KafkaAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_KEYCLOAK = "keycloak";

    public static final String AUTHORIZER_CLASS_NAME = "io.strimzi.kafka.oauth.server.authorizer.KeycloakRBACAuthorizer";
    public static final String PRINCIPAL_BUILDER_CLASS_NAME = "io.strimzi.kafka.oauth.server.authorizer.JwtKafkaPrincipalBuilder";

    private String clientId;
    private String tokenEndpointUri;
    private List<CertSecretSource> tlsTrustedCertificates;
    private boolean disableTlsHostnameVerification = false;
    private boolean delegateToKafkaAcls = false;
    private List<String> superUsers;

    @Description("Must be `" + TYPE_KEYCLOAK + "`")
    @Override
    public String getType() {
        return TYPE_KEYCLOAK;
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
            "Default value is `false`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isDelegateToKafkaAcls() {
        return delegateToKafkaAcls;
    }

    public void setDelegateToKafkaAcls(boolean delegateToKafkaAcls) {
        this.delegateToKafkaAcls = delegateToKafkaAcls;
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
}
