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

/**
 * Class containing methods for handling the creation of {@link Authentication} object with configuration of
 * particular authentication. These methods are used in the Test-Clients builders for easier manipulation and creation
 * of desired clients.
 */
public class ClientsAuthentication {
    /**
     * Creates {@link Authentication} for SCRAM-SHA-512 over TLS.
     *
     * @param namespaceName     Name of the Namespace where the KafkaUser's Secret with `sasl.jaas.config` is present.
     * @param userName          Name of the KafkaUser for which we should obtain the data.
     * @param clusterName       Name of the Kafka cluster for which the cluster CA certificate is obtained.
     *
     * @return  configured {@link Authentication} with SCRAM-SHA-512 over TLS.
     */
    public static Authentication configureTlsScramSha(String namespaceName, String userName, String clusterName) {
        return configureScramSha(namespaceName, userName, SecurityProtocol.SASL_SSL)
            .withNewSsl()
                .withSslTruststoreCertificate(KafkaResources.clusterCaCertificateSecretName(clusterName))
            .endSsl()
            .build();
    }

    /**
     * Creates {@link Authentication} for SCRAM-SHA-512 over PLAIN.
     *
     * @param namespaceName     Name of the Namespace where the KafkaUser's Secret with `sasl.jaas.config` is present.
     * @param userName          Name of the KafkaUser for which we should obtain the data.
     *
     * @return  configured {@link Authentication} with SCRAM-SHA-512 over PLAIN.
     */
    public static Authentication configurePlainScramSha(String namespaceName, String userName) {
        return configureScramSha(namespaceName, userName, SecurityProtocol.SASL_PLAINTEXT).build();
    }

    /**
     * Default method for creating {@link AuthenticationBuilder} with SCRAM-SHA-512.
     * This can be then configured to have a different security protocol and then extended with other configuration
     * - for example the SSL config.
     *
     * @param namespaceName     Name of the Namespace where the KafkaUser's Secret with `sasl.jaas.config` is present.
     * @param userName          Name of the KafkaUser for which we should obtain the data.
     *
     * @return  configured {@link AuthenticationBuilder} with SCRAM-SHA-512.
     */
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

    /**
     * Creates {@link Authentication} for TLS - mainly created for usual Kafka cluster CA and TLS KafkaUser.
     *
     * @param clusterName   Name of the Kafka cluster for which the cluster CA should be obtained.
     * @param userName      Name of the KafkaUser for which the keystore cert and key are obtained.
     *
     * @return  configured {@link Authentication} with TLS configuration.
     */
    public static Authentication configureTls(String clusterName, String userName) {
        return configureTls(KafkaResources.clusterCaCertificateSecretName(clusterName), userName, userName);
    }

    /**
     * Creates {@link Authentication} for TLS with custom CA certificate and keystore Secret.
     *
     * @param caCertificateSecretName   Name of the custom CA certificate Secret.
     * @param keystoreSecretName        Name of the keystore certificate Secret.
     *
     * @return  configured {@link Authentication} with TLS configuration.
     */
    public static Authentication configureTlsCustomCerts(String caCertificateSecretName, String keystoreSecretName) {
        return configureTls(caCertificateSecretName, keystoreSecretName, keystoreSecretName);
    }

    /**
     * Default configuration method for configuring {@link Authentication} with TLS.
     * Truststore and Keystore can be configured with different Secrets - mainly in cases when we have multiple custom certificates.
     *
     * @param caCertificateSecretName               Name of the Secret containing CA certificate.
     * @param keystoreKeySecretName                 Name of the Secret containing keystore key.
     * @param keystoreCertificateChainSecretName    Name of the Secret containing keystore certificate chain.
     *
     * @return  configured {@link Authentication} with TLS configuration.
     */
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

    /**
     * Creates {@link Authentication} for TLS OAuth.
     *
     * @param clusterName               Name of the Kafka cluster for which the cluster CA is obtained.
     * @param oauthClientId             Id of the OAuth client that is used for keystore, but also for OAuth related configuration.
     * @param oauthClientSecret         Name of the OAuth client Secret.
     * @param oauthTokenEndpointUri     Endpoint URI for obtaining the OAuth token.
     *
     * @return  configured {@link Authentication} with TLS OAuth configuration.
     */
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

    /**
     * Creates {@link Authentication} with OAuth over plain.
     *
     * @param oauthClientId             Id of the OAuth client that is used for OAuth related configuration.
     * @param oauthClientSecret         Name of the OAuth client Secret.
     * @param oauthTokenEndpointUri     Endpoint URI for obtaining the OAuth token.
     *
     * @return  configured {@link Authentication} with Plain OAuth configuration.
     */
    public static Authentication configureOAuthPlain(String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri) {
        return configureOAuth(oauthClientId, oauthClientSecret, oauthTokenEndpointUri, null).build();
    }

    /**
     * Default configuration method for configuring {@link AuthenticationBuilder} with OAuth.
     *
     * @param oauthClientId             Id of the OAuth client that is used for OAuth related configuration.
     * @param oauthClientSecret         Name of the OAuth client Secret.
     * @param oauthTokenEndpointUri     Endpoint URI for obtaining the OAuth token.
     * @param additionalEnvVars         Additional OAuth related environment variables.
     *
     * @return configured {@link AuthenticationBuilder} with basic OAuth configuration.
     */
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
