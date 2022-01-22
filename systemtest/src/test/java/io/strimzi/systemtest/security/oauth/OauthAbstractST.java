/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.templates.kubernetes.NetworkPolicyTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Base64;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(OAUTH)
@Tag(REGRESSION)
public class OauthAbstractST extends AbstractST {

    protected static final Logger LOGGER = LogManager.getLogger(OauthAbstractST.class);
    protected static final String OAUTH_CLIENT_NAME = "hello-world-producer";
    protected static final String OAUTH_CLIENT_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_CLIENT_AUDIENCE_PRODUCER = "kafka-audience-producer";
    protected static final String OAUTH_CLIENT_AUDIENCE_CONSUMER = "kafka-audience-consumer";
    protected static final String OAUTH_CLIENT_AUDIENCE_SECRET = "kafka-audience-secret";

    protected static final String OAUTH_KAFKA_BROKER_NAME = "kafka-broker";
    protected static final String OAUTH_KAFKA_CLIENT_NAME = "kafka-client";
    protected static final String OAUTH_PRODUCER_NAME = "oauth-producer";
    protected static final String OAUTH_CONSUMER_NAME = "oauth-consumer";

    protected static final String CONNECT_OAUTH_SECRET = "my-connect-oauth";
    protected static final String MIRROR_MAKER_OAUTH_SECRET = "my-mirror-maker-oauth";
    protected static final String MIRROR_MAKER_2_OAUTH_SECRET = "my-mirror-maker-2-oauth";
    protected static final String BRIDGE_OAUTH_SECRET = "my-bridge-oauth";
    protected static final String OAUTH_KAFKA_BROKER_SECRET = "kafka-broker-secret";
    protected static final String OAUTH_KAFKA_PRODUCER_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_KAFKA_CONSUMER_SECRET = "hello-world-consumer-secret";
    protected static final String OAUTH_TEAM_A_SECRET = "team-a-client-secret";
    protected static final String OAUTH_TEAM_B_SECRET = "team-b-client-secret";
    protected static final String OAUTH_KAFKA_CLIENT_SECRET = "kafka-client-secret";
    protected static final String OAUTH_KEY = "clientSecret";

    protected final String audienceListenerPort = "9098";

    protected KeycloakInstance keycloakInstance;

    public static Map<String, Object> connectorConfig;
    static {
        connectorConfig = new HashMap<>();
        connectorConfig.put("config.storage.replication.factor", -1);
        connectorConfig.put("config.topic.cleanup.policy", "compact");
        connectorConfig.put("offset.storage.replication.factor", -1);
        connectorConfig.put("status.storage.replication.factor", -1);
    }

    protected static final Function<KeycloakInstance, GenericKafkaListener> BUILD_OAUTH_TLS_LISTENER = (keycloakInstance) -> {
        return new GenericKafkaListenerBuilder()
            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .withPort(9093)
            .withType(KafkaListenerType.INTERNAL)
            .withTls(true)
            .withNewKafkaListenerAuthenticationOAuth()
                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                .withTlsTrustedCertificates(
                    new CertSecretSourceBuilder()
                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                        .build())
                .withDisableTlsHostnameVerification(true)
            .endKafkaListenerAuthenticationOAuth()
            .build();
    };

    protected void setupCoAndKeycloak(ExtensionContext extensionContext, String namespace) {
        clusterOperator.unInstall();
        clusterOperator.defaultInstallation().createInstallation().runInstallation();

        resourceManager.createResource(extensionContext, NetworkPolicyTemplates.applyDefaultNetworkPolicy(extensionContext, namespace, DefaultNetworkPolicy.DEFAULT_TO_ALLOW));

        LOGGER.info("Deploying keycloak...");

        KeycloakUtils.deployKeycloak(namespace);

        SecretUtils.waitForSecretReady(namespace, "credential-example-keycloak", () -> { });
        String passwordEncoded = kubeClient(namespace).getSecret(namespace, "credential-example-keycloak").getData().get("ADMIN_PASSWORD");
        String password = new String(Base64.getDecoder().decode(passwordEncoded.getBytes()));
        keycloakInstance = new KeycloakInstance("admin", password, namespace);

        createSecretsForDeployments(namespace);
    }

    @AfterEach
    void tearDownEach(ExtensionContext extensionContext) throws Exception {
        List<Job> clusterJobList = kubeClient().getJobList().getItems()
            .stream()
            .filter(
                job -> job.getMetadata().getName().contains(mapWithClusterNames.get(extensionContext.getDisplayName())))
            .collect(Collectors.toList());

        for (Job job : clusterJobList) {
            LOGGER.info("Deleting {} job", job.getMetadata().getName());
            JobUtils.deleteJobWithWait(job.getMetadata().getNamespace(), job.getMetadata().getName());
        }
    }

    /**
     * Auxiliary method, which creating kubernetes secrets
     * f.e name kafka-broker-secret -> will be encoded to base64 format and use as a key.
     */
    private void createSecretsForDeployments(final String namespace) {
        SecretUtils.createSecret(namespace, OAUTH_KAFKA_PRODUCER_SECRET, OAUTH_KEY, "hello-world-producer-secret");
        SecretUtils.createSecret(namespace, OAUTH_KAFKA_CONSUMER_SECRET, OAUTH_KEY, "hello-world-streams-secret");
        SecretUtils.createSecret(namespace, OAUTH_TEAM_A_SECRET, OAUTH_KEY, "team-a-client-secret");
        SecretUtils.createSecret(namespace, OAUTH_TEAM_B_SECRET, OAUTH_KEY, "team-b-client-secret");
        SecretUtils.createSecret(namespace, OAUTH_KAFKA_BROKER_SECRET, OAUTH_KEY, "kafka-broker-secret");
        SecretUtils.createSecret(namespace, CONNECT_OAUTH_SECRET, OAUTH_KEY, "kafka-connect-secret");
        SecretUtils.createSecret(namespace, MIRROR_MAKER_OAUTH_SECRET, OAUTH_KEY, "kafka-mirror-maker-secret");
        SecretUtils.createSecret(namespace, MIRROR_MAKER_2_OAUTH_SECRET, OAUTH_KEY, "kafka-mirror-maker-2-secret");
        SecretUtils.createSecret(namespace, BRIDGE_OAUTH_SECRET, OAUTH_KEY, "kafka-bridge-secret");
        SecretUtils.createSecret(namespace, OAUTH_CLIENT_AUDIENCE_SECRET, OAUTH_KEY, "kafka-audience-secret");
        SecretUtils.createSecret(namespace, OAUTH_KAFKA_CLIENT_SECRET, OAUTH_KEY, "kafka-client-secret");
    }
}


