/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.oauth;

import io.fabric8.kubernetes.api.model.Service;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.MessagingBaseST;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.executor.Exec;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Resources.deployKeycloakNodePortService;

@Tag(NODEPORT_SUPPORTED)
@Tag(REGRESSION)
@ExtendWith(VertxExtension.class)
public class OauthST extends MessagingBaseST {

    public static final String NAMESPACE = "oauth2-cluster-test";
    protected static final Logger LOGGER = LogManager.getLogger(OauthST.class);

    protected static final String TOPIC_NAME = "my-topic";
    static final String REVERSE_TOPIC_NAME = "my-topic-reversed";

    static final String PRODUCER_USER_NAME = "hello-world-producer";
    static final String CONSUMER_USER_NAME = "hello-world-consumer";
    static final String STREAMS_USER_NAME = "hello-world-streams";

    private static final String PRODUCER_OAUTH_SECRET = "hello-world-producer-oauth";
    private static final String CONSUMER_OAUTH_SECRET = "hello-world-consumer-oauth";
    private static final String STREAMS_OAUTH_SECRET = "hello-world-streams-oauth";
    static final String CONNECT_OAUTH_SECRET = "my-connect-oauth";
    static final String MIRROR_MAKER_OAUTH_SECRET = "my-mirror-maker-oauth";
    static final String BRIDGE_OAUTH_SECRET = "my-bridge-oauth";
    static final String OAUTH_KEY = "clientSecret";

    static String oauthTokenEndpointUri;
    static String validIssuerUri;
    static String jwksEndpointUri;
    static String userNameClaim;
    static final String CERTIFICATE_OF_KEYCLOAK = "tls.crt";
    static final String SECRET_OF_KEYCLOAK = "x509-https-secret";

    static String clusterHost = "";
    static final String BRIDGE_EXTERNAL_SERVICE = CLUSTER_NAME + "-bridge-external-service";
    protected WebClient client;

    JsonObject sendHttpRequests(JsonObject records, String bridgeHost, int bridgePort) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Sending records to Kafka Bridge");
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost, "/topics/" + TOPIC_NAME)
                .putHeader("Content-length", String.valueOf(records.toBuffer().length()))
                .putHeader("Content-Type", Constants.KAFKA_BRIDGE_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(records, ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<JsonObject> response = ar.result();
                        if (response.statusCode() == HttpResponseStatus.OK.code()) {
                            LOGGER.debug("Server accepted post");
                            future.complete(response.body());
                        } else {
                            LOGGER.error("Server didn't accept post", ar.cause());
                        }
                    } else {
                        LOGGER.error("Server didn't accept post", ar.cause());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    protected int getBridgeNodePort() {
        Service extBootstrapService = kubeClient(NAMESPACE).getClient().services()
                .inNamespace(NAMESPACE)
                .withName(BRIDGE_EXTERNAL_SERVICE)
                .get();

        return extBootstrapService.getSpec().getPorts().get(0).getNodePort();
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() throws InterruptedException {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();

        LOGGER.info("Deploying keycloak...");
        testClassResources().deployKeycloak().done();

        deployKeycloakNodePortService(NAMESPACE);

        StUtils.waitForNodePortService("keycloakservice-https");

        clusterHost = kubeClient(NAMESPACE).getNodeAddress();

        String keycloakIpWithPort = clusterHost + ":32223";

        LOGGER.info("Importing new realm");
        Exec.exec(true, "/bin/bash", "../systemtest/src/test/resources/oauth2/create_realm.sh", "admin", "admin", keycloakIpWithPort);

        validIssuerUri = "https://" + keycloakIpWithPort + "/auth/realms/internal";
        jwksEndpointUri = "https://" + keycloakIpWithPort + "/auth/realms/internal/protocol/openid-connect/certs";
        oauthTokenEndpointUri = "https://" + keycloakIpWithPort + "/auth/realms/internal/protocol/openid-connect/token";
        userNameClaim = "preferred_username";

        String keycloakPodName = kubeClient().listPodsByPrefixInName("keycloak-").get(0).getMetadata().getName();

        String pubKey = cmdKubeClient().execInPod(keycloakPodName, "keytool", "-exportcert", "-keystore",
                "/opt/jboss/keycloak/standalone/configuration/application.keystore", "-alias", "server", "-storepass", "password", "-rfc").out();

        StUtils.createSecret(SECRET_OF_KEYCLOAK, CERTIFICATE_OF_KEYCLOAK, new String(Base64.getEncoder().encode(pubKey.getBytes()), StandardCharsets.US_ASCII));

        testClassResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewPlain()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withJwksExpirySeconds(500)
                                    .withJwksRefreshSeconds(400)
                                    .withUserNameClaim(userNameClaim)
                                    .withTlsTrustedCertificates(
                                        new CertSecretSourceBuilder()
                                            .withSecretName(SECRET_OF_KEYCLOAK)
                                            .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                            .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                            .endPlain()
                            .withNewTls()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withJwksExpirySeconds(500)
                                    .withJwksRefreshSeconds(400)
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
                                    .withJwksExpirySeconds(500)
                                    .withJwksRefreshSeconds(400)
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

        testClassResources().topic(CLUSTER_NAME, TOPIC_NAME).done();
        testClassResources().topic(CLUSTER_NAME, REVERSE_TOPIC_NAME).done();

        createSecretsForDeployments();

        testClassResources().tlsUser(CLUSTER_NAME, PRODUCER_USER_NAME).done();
        testClassResources().tlsUser(CLUSTER_NAME, CONSUMER_USER_NAME).done();
        testClassResources().tlsUser(CLUSTER_NAME, STREAMS_USER_NAME).done();
    }

    private void createSecretsForDeployments() {
        StUtils.createSecret(PRODUCER_OAUTH_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0");
        StUtils.createSecret(CONSUMER_OAUTH_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtY29uc3VtZXItc2VjcmV0");
        StUtils.createSecret(STREAMS_OAUTH_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtc3RyZWFtcy1zZWNyZXQ=");
        StUtils.createSecret(CONNECT_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtY29ubmVjdC1zZWNyZXQ=");
        StUtils.createSecret(MIRROR_MAKER_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLXNlY3JldA==");
        StUtils.createSecret(BRIDGE_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtYnJpZGdlLXNlY3JldA==");
    }
}
