/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;

import java.util.Base64;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
@ExtendWith(VertxExtension.class)
public class OauthAbstractST extends AbstractST {

    public static final String NAMESPACE = "oauth2-cluster-test";
    protected static final Logger LOGGER = LogManager.getLogger(OauthAbstractST.class);
    protected static final String OAUTH_CLIENT_NAME = "hello-world-producer";
    protected static final String OAUTH_CLIENT_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_KAFKA_CLIENT_NAME = "kafka-broker";

    protected static final String CONNECT_OAUTH_SECRET = "my-connect-oauth";
    protected static final String MIRROR_MAKER_OAUTH_SECRET = "my-mirror-maker-oauth";
    protected static final String MIRROR_MAKER_2_OAUTH_SECRET = "my-mirror-maker-2-oauth";
    protected static final String BRIDGE_OAUTH_SECRET = "my-bridge-oauth";
    protected static final String OAUTH_KAFKA_CLIENT_SECRET = "kafka-broker-secret";
    protected static final String OAUTH_KEY = "clientSecret";

    protected KeycloakInstance keycloakInstance;

    protected static final String CERTIFICATE_OF_KEYCLOAK = "tls.crt";
    protected static final String SECRET_OF_KEYCLOAK = "sso-x509-https-secret";

    protected static String clusterHost;
    protected static final String BRIDGE_EXTERNAL_SERVICE = CLUSTER_NAME + "-bridge-external-service";
    protected WebClient client;

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
        KubernetesResource.applyDefaultNetworkPolicy(NAMESPACE, DefaultNetworkPolicy.DEFAULT_TO_ALLOW);

        LOGGER.info("Deploying keycloak...");

        KeycloakUtils.deployKeycloak(NAMESPACE);

        // https
        Service keycloakService = KubernetesResource.createKeycloakNodePortService(NAMESPACE);
        KubernetesResource.createServiceResource(keycloakService, NAMESPACE);
        ServiceUtils.waitForNodePortService(keycloakService.getMetadata().getName());

        // http
        Service keycloakHttpService = KubernetesResource.createKeycloakNodePortHttpService(NAMESPACE);
        KubernetesResource.createServiceResource(keycloakHttpService, NAMESPACE);
        ServiceUtils.waitForNodePortService(keycloakHttpService.getMetadata().getName());

        String passwordEncoded = kubeClient().getSecret("credential-example-keycloak").getData().get("ADMIN_PASSWORD");
        String password = new String(Base64.getDecoder().decode(passwordEncoded.getBytes()));
        keycloakInstance = new KeycloakInstance("admin", password, NAMESPACE);

        clusterHost = kubeClient().getNodeAddress();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(SECRET_OF_KEYCLOAK)
                                        .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                        .build())
                                .withDisableTlsHostnameVerification(true)
                            .endKafkaListenerAuthenticationOAuth()
                        .endTls()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(SECRET_OF_KEYCLOAK)
                                        .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                        .build())
                                .withDisableTlsHostnameVerification(true)
                            .endKafkaListenerAuthenticationOAuth()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        createSecretsForDeployments();

        KafkaUserResource.tlsUser(CLUSTER_NAME, OAUTH_CLIENT_NAME).done();
    }

    /**
     * Auxiliary method, which creating kubernetes secrets
     * f.e name kafka-broker-secret -> will be encoded to base64 format and use as a key.
     */
    private void createSecretsForDeployments() {
        SecretUtils.createSecret(OAUTH_KAFKA_CLIENT_SECRET, OAUTH_KEY, "a2Fma2EtYnJva2VyLXNlY3JldA==");
        SecretUtils.createSecret(CONNECT_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtY29ubmVjdC1zZWNyZXQ=");
        SecretUtils.createSecret(MIRROR_MAKER_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLXNlY3JldA==");
        SecretUtils.createSecret(MIRROR_MAKER_2_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLTItc2VjcmV0");
        SecretUtils.createSecret(BRIDGE_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtYnJpZGdlLXNlY3JldA==");
    }
}


