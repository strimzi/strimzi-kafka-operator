/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
class HttpBridgeExternalListenersST extends HttpBridgeAbstractST {
    private static final String BRIDGE_EXTERNAL_SERVICE = CLUSTER_NAME + "-bridge-external-service";
    private static final String NAMESPACE = "bridge-external-cluster-test";

    @Test
    void testScramShaAuthWithWeirdUsername() throws Exception {
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withNewUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, SecurityProtocol.SASL_SSL);
    }

    @Test
    void testTlsAuthWithWeirdUsername() throws Exception {
        // Create weird named user with . and maximum of 64 chars -> TLS
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationTls()
                .withNewCertificateAndKey()
                    .withSecretName(weirdUserName)
                    .withCertificate("user.crt")
                    .withKey("user.key")
                .endCertificateAndKey()
            .endKafkaClientAuthenticationTls()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, SecurityProtocol.SSL);
    }

    private void testWeirdUsername(String weirdUserName, KafkaListenerAuthentication auth, KafkaBridgeSpec spec, SecurityProtocol securityProtocol) throws Exception {
        String aliceUser = "alice";
        String groupId = "my-group-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withAuth(auth)
                        .endKafkaListenerExternalNodePort()
                        .withNewTls()
                            .withAuth(auth)
                        .endTls()
                    .endListeners()
                .endKafka()
                .endSpec()
            .done();

        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        // Create user
        if (auth.getType().equals("tls")) {
            KafkaUserResource.tlsUser(CLUSTER_NAME, weirdUserName).done();
            KafkaUserResource.tlsUser(CLUSTER_NAME, aliceUser).done();
        } else {
            KafkaUserResource.scramShaUser(CLUSTER_NAME, weirdUserName).done();
            KafkaUserResource.scramShaUser(CLUSTER_NAME, aliceUser).done();
        }

        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1)
            .withNewSpecLike(spec)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                .withReplicas(1)
                .withNewInlineLogging()
                    .addToLoggers("bridge.root.logger", "DEBUG")
                .endInlineLogging()
                .withNewHttp(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .endSpec()
            .done();

        Service service = KafkaBridgeUtils.createBridgeNodePortService(CLUSTER_NAME, NAMESPACE, BRIDGE_EXTERNAL_SERVICE);
        KubernetesResource.createServiceResource(service, NAMESPACE).done();

        bridgeHost = kubeClient(NAMESPACE).getNodeAddress();
        bridgePort = KafkaBridgeUtils.getBridgeNodePort(NAMESPACE, BRIDGE_EXTERNAL_SERVICE);

        JsonObject config = new JsonObject();
        config.put("name", aliceUser);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        JsonObject response = BridgeUtils.createHttpConsumer(config, bridgeHost, bridgePort, groupId, client);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(aliceUser));

        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(TOPIC_NAME);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);

        // Subscribe
        assertThat(BridgeUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, aliceUser, client), is(true));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(weirdUserName)
            .withSecurityProtocol(securityProtocol)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));
        // Try to consume messages
        JsonArray bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, groupId, aliceUser, client);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, groupId, aliceUser, client);
        }
        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(MESSAGE_COUNT));
        // Delete consumer
        assertThat(BridgeUtils.deleteConsumer(bridgeHost, bridgePort, groupId, aliceUser, client), is(true));
    }

    @BeforeAll
    void createClassResources(Vertx vertx) throws Exception {
        deployClusterOperator(NAMESPACE);
        // Create http client
        client = WebClient.create(vertx, new WebClientOptions()
            .setSsl(false));
    }
}
