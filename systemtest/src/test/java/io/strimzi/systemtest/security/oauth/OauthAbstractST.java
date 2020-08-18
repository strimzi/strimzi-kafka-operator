/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.executor.Exec;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
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

    protected static String oauthTokenEndpointUri;
    protected static String validIssuerUri;
    protected static String jwksEndpointUri;
    protected static String introspectionEndpointUri;
    protected static String userNameClaim;
    protected static final int JWKS_EXPIRE_SECONDS = 500;
    protected static final int JWKS_REFRESH_SECONDS = 400;
    protected static final int MESSAGE_COUNT = 100;

    protected static final String CERTIFICATE_OF_KEYCLOAK = "tls.crt";
    protected static final String SECRET_OF_KEYCLOAK = "x509-https-secret";

    protected static String clusterHost;
    protected static String keycloakIpWithPortHttp;
    protected static String keycloakIpWithPortHttps;
    protected static final String BRIDGE_EXTERNAL_SERVICE = CLUSTER_NAME + "-bridge-external-service";
    protected WebClient client;

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
        KubernetesResource.applyDefaultNetworkPolicy(NAMESPACE, DefaultNetworkPolicy.DEFAULT_TO_ALLOW);

        deployTestSpecificResources();
    }

    private void deployTestSpecificResources() throws InterruptedException {
        LOGGER.info("Deploying keycloak...");
        KafkaClientsResource.deployKeycloak().done();

        // https
        Service keycloakService = KubernetesResource.deployKeycloakNodePortService(NAMESPACE);

        KubernetesResource.createServiceResource(keycloakService, NAMESPACE);
        ServiceUtils.waitForNodePortService(keycloakService.getMetadata().getName());

        // http
        Service keycloakHttpService = KubernetesResource.deployKeycloakNodePortHttpService(NAMESPACE);

        KubernetesResource.createServiceResource(keycloakHttpService, NAMESPACE);
        ServiceUtils.waitForNodePortService(keycloakHttpService.getMetadata().getName());

        clusterHost = kubeClient(NAMESPACE).getNodeAddress();

        keycloakIpWithPortHttps = clusterHost + ":" + Constants.HTTPS_KEYCLOAK_DEFAULT_NODE_PORT;
        keycloakIpWithPortHttp = clusterHost + ":" + Constants.HTTP_KEYCLOAK_DEFAULT_NODE_PORT;

        LOGGER.info("Importing new realm");
        Exec.exec(true, "/bin/bash", "../systemtest/src/test/resources/oauth2/create_realm.sh", "admin", "admin", keycloakIpWithPortHttps);
        Exec.exec(true, "/bin/bash", "../systemtest/src/test/resources/oauth2/create_realm_authorization.sh", "admin", "admin", keycloakIpWithPortHttps);

        validIssuerUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/internal";
        jwksEndpointUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/internal/protocol/openid-connect/certs";
        oauthTokenEndpointUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/internal/protocol/openid-connect/token";
        introspectionEndpointUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/internal/protocol/openid-connect/token/introspect";
        userNameClaim = "preferred_username";

        String keycloakPodName = kubeClient().listPodsByPrefixInName("keycloak-").get(0).getMetadata().getName();

        String pubKey = cmdKubeClient().execInPod(keycloakPodName, "keytool", "-exportcert", "-keystore",
                "/opt/jboss/keycloak/standalone/configuration/application.keystore", "-alias", "server", "-storepass", "password", "-rfc").out();

        SecretUtils.createSecret(SECRET_OF_KEYCLOAK, CERTIFICATE_OF_KEYCLOAK, new String(Base64.getEncoder().encode(pubKey.getBytes()), StandardCharsets.US_ASCII));

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewTls()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksExpirySeconds(JWKS_EXPIRE_SECONDS)
                                    .withJwksRefreshSeconds(JWKS_REFRESH_SECONDS)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withUserNameClaim(userNameClaim)
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
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksExpirySeconds(JWKS_EXPIRE_SECONDS)
                                    .withJwksRefreshSeconds(JWKS_REFRESH_SECONDS)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withUserNameClaim(userNameClaim)
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


