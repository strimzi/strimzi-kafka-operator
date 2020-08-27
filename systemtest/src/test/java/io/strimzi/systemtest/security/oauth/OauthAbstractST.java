/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.batch.Job;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@ExtendWith(VertxExtension.class)
public class OauthAbstractST extends AbstractST {

    public static final String NAMESPACE = "oauth2-cluster-test";
    protected static final Logger LOGGER = LogManager.getLogger(OauthAbstractST.class);
    protected static final String OAUTH_CLIENT_NAME = "hello-world-producer";
    protected static final String OAUTH_CLIENT_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_KAFKA_CLIENT_NAME = "kafka-broker";
    protected static final String OAUTH_PRODUCER_NAME = "oauth-producer";
    protected static final String OAUTH_CONSUMER_NAME = "oauth-consumer";

    protected static final String CONNECT_OAUTH_SECRET = "my-connect-oauth";
    protected static final String MIRROR_MAKER_OAUTH_SECRET = "my-mirror-maker-oauth";
    protected static final String MIRROR_MAKER_2_OAUTH_SECRET = "my-mirror-maker-2-oauth";
    protected static final String BRIDGE_OAUTH_SECRET = "my-bridge-oauth";
    protected static final String OAUTH_KAFKA_CLIENT_SECRET = "kafka-broker-secret";
    protected static final String OAUTH_KAFKA_PRODUCER_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_KAFKA_CONSUMER_SECRET = "hello-world-consumer-secret";
    protected static final String OAUTH_KEY = "clientSecret";

    protected KeycloakInstance keycloakInstance;

    protected static final String CERTIFICATE_OF_KEYCLOAK = "tls.crt";
    protected static final String SECRET_OF_KEYCLOAK = "x509-https-secret";

    protected static String clusterHost;
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

        // TODO: ====== this should be removed ??
//        LOGGER.info("Importing basic realm");
//        keycloakInstance.importRealm("../systemtest/src/test/resources/oauth2/create_realm.sh");
//
//        LOGGER.info("Importing authorization realm");
//
//        keycloakInstance.importRealm("../systemtest/src/test/resources/oauth2/create_realm_authorization.sh");

        String keycloakPodName = kubeClient().listPodsByPrefixInName("keycloak-").get(0).getMetadata().getName();

        String pubKey = ResourceManager.cmdKubeClient().execInPod(keycloakPodName, "keytool", "-exportcert", "-keystore",
            "/opt/jboss/keycloak/standalone/configuration/application.keystore", "-alias", "server", "-storepass", "password", "-rfc").out();

        SecretUtils.createSecret(SECRET_OF_KEYCLOAK, CERTIFICATE_OF_KEYCLOAK, new String(Base64.getEncoder().encode(pubKey.getBytes()), StandardCharsets.US_ASCII));

        // TODO: ======= until here ?

        createSecretsForDeployments();
    }

    @AfterEach
    void tearDown() {

        for (Job job : kubeClient().getClient().batch().jobs().inNamespace(NAMESPACE).list().getItems()) {
            LOGGER.info("Deleting {} job", job.getMetadata().getName());
            kubeClient().getClient().batch().jobs().inNamespace(NAMESPACE).delete(job);
        }
    }

    /**
     * Auxiliary method, which creating kubernetes secrets
     * f.e name kafka-broker-secret -> will be encoded to base64 format and use as a key.
     */
    private void createSecretsForDeployments() {
        SecretUtils.createSecret(OAUTH_KAFKA_PRODUCER_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0");
        SecretUtils.createSecret(OAUTH_KAFKA_CONSUMER_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtc3RyZWFtcy1zZWNyZXQ=");
        SecretUtils.createSecret(OAUTH_KAFKA_CLIENT_SECRET, OAUTH_KEY, "a2Fma2EtYnJva2VyLXNlY3JldA==");
        SecretUtils.createSecret(CONNECT_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtY29ubmVjdC1zZWNyZXQ=");
        SecretUtils.createSecret(MIRROR_MAKER_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLXNlY3JldA==");
        SecretUtils.createSecret(MIRROR_MAKER_2_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLTItc2VjcmV0");
        SecretUtils.createSecret(BRIDGE_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtYnJpZGdlLXNlY3JldA==");
    }
}


