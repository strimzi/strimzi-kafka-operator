/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.testclients.configuration.Authentication;
import io.strimzi.testclients.configuration.AuthenticationBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.ArrayList;
import java.util.List;

public class ClientsAuthentication {
    public static Authentication configureTlsScramSha(String namespaceName, String userName, String clusterName) {
        return configureScramSha(namespaceName, userName, SecurityProtocol.SASL_SSL)
            .withNewSsl()
                .withSslTruststoreCertificate(KafkaResources.clusterCaCertificateSecretName(clusterName))
            .endSsl()
            .build();
    }

    public static Authentication configurePlainScramSha(String namespaceName, String userName) {
        return configureScramSha(namespaceName, userName, SecurityProtocol.SASL_PLAINTEXT).build();
    }

    public static AuthenticationBuilder configureScramSha(String namespaceName, String userName, SecurityProtocol securityProtocol) {
        final String saslJaasConfigEncrypted = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(userName).get().getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = Util.decodeFromBase64(saslJaasConfigEncrypted);

        return new AuthenticationBuilder()
            .withNewSasl()
                .withSaslJaasConfig(saslJaasConfigDecrypted)
                .withSaslMechanism("SCRAM-SHA-512")
            .endSasl()
            .withSecurityProtocol(securityProtocol.toString());
    }

    public static Authentication configureTls(String clusterName, String userName) {
        return configureTls(KafkaResources.clusterCaCertificateSecretName(clusterName), userName, userName);
    }

    public static Authentication configureTlsCustomCerts(String caCertificateSecretName, String keystoreSecretName) {
        return configureTls(caCertificateSecretName, keystoreSecretName, keystoreSecretName);
    }

    private static Authentication configureTls(String caCertificateSecretName, String keystoreKeySecretName, String keystoreCertificateChainSecretName) {
        return new AuthenticationBuilder()
            .withNewSsl()
                .withSslTruststoreCertificate(caCertificateSecretName)
                .withSslKeystoreKey(keystoreKeySecretName)
                .withSslKeystoreCertificateChain(keystoreCertificateChainSecretName)
            .endSsl()
            .withNewSasl()
                .withSaslMechanism("GSSAPI")
            .endSasl()
            .withSecurityProtocol(SecurityProtocol.SSL.toString())
            .build();
    }

    public static Authentication configureTlsOAuth(String clusterName, String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri) {
        EnvVar oauthSslEndpointEnvVar = new EnvVarBuilder()
                .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM")
                .withValue("")
                .build();

        return configureOAuth(oauthClientId, oauthClientSecret, oauthTokenEndpointUri, List.of(oauthSslEndpointEnvVar))
            .withNewSsl()
                .withSslTruststoreCertificate(KafkaResources.clusterCaCertificateSecretName(clusterName))
                .withSslKeystoreKey(oauthClientId)
                .withSslKeystoreCertificateChain(oauthClientId)
            .endSsl()
            .build();
    }

    public static Authentication configureOAuthPlain(String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri) {
        return configureOAuth(oauthClientId, oauthClientSecret, oauthTokenEndpointUri, null).build();
    }

    private static AuthenticationBuilder configureOAuth(String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri, List<EnvVar> additionalEnvVars) {
        List<EnvVar> envVars = new ArrayList<>(List.of(
            new EnvVarBuilder()
                .withName("OAUTH_SSL_TRUSTSTORE_CERTIFICATES")
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                        .withKey(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                    .endSecretKeyRef()
                .endValueFrom()
                .build(),
            new EnvVarBuilder()
                .withName("OAUTH_SSL_TRUSTSTORE_TYPE")
                .withValue("PEM")
                .build(),
            new EnvVarBuilder()
                .withName("OAUTH_CLIENT_SECRET")
                .editOrNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(oauthClientSecret)
                        .withKey("clientSecret")
                    .endSecretKeyRef()
                .endValueFrom()
                .build()
        ));

        if (additionalEnvVars != null) {
            envVars.addAll(additionalEnvVars);
        }

        return new AuthenticationBuilder()
            .withNewOauth()
                .withOauthClientId(oauthClientId)
                .withOauthTokenEndpointUri(oauthTokenEndpointUri)
                .withAdditionalOAuthEnvVars(envVars)
            .endOauth();
    }
}
