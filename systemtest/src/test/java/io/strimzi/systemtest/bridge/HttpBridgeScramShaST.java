/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.bridge.HttpBridgeST.NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(BRIDGE)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
@ExtendWith(VertxExtension.class)
class HttpBridgeScramShaST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeScramShaST.class);

    private String bridgeHost = "";
    private int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;

    @Test
    void testSendSimpleMessageTlsScramSha() throws Exception {
        int messageCount = 50;
        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        JsonObject records = BridgeUtils.generateHttpMessages(messageCount);
        JsonObject response = BridgeUtils.sendMessagesHttpRequest(records, bridgeHost, bridgePort, TOPIC_NAME, client);
        KafkaBridgeUtils.checkSendResponse(response, messageCount);

        BasicExternalKafkaClient kafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(messageCount)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .build();

        assertThat(kafkaClient.receiveMessagesTls(), is(messageCount));
    }

    @Test
    void testReceiveSimpleMessageTlsScramSha() throws Exception {
        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        BasicExternalKafkaClient kafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .build();

        // Send messages to Kafka
        assertThat(kafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        String name = "kafka-consumer-simple-receive";

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create consumer
        JsonObject response = BridgeUtils.createBridgeConsumer(config, bridgeHost, bridgePort, CONSUMER_GROUP_NAME, client);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(name));
        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(TOPIC_NAME);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);
        // Subscribe
        assertThat(BridgeUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, CONSUMER_GROUP_NAME, name, client), is(true));
        // Try to consume messages
        JsonArray bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, CONSUMER_GROUP_NAME, name, client);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, CONSUMER_GROUP_NAME, name, client);
        }

        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(MESSAGE_COUNT));
        // Delete consumer
        assertThat(BridgeUtils.deleteConsumer(bridgeHost, bridgePort, CONSUMER_GROUP_NAME, name, client), is(true));
    }

    @Test
    void testScramShaAuthWithWeirdNamedUser() throws Exception {
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";
        // Create user with normal name -> we don't need to set weird name for consumer
        String aliceUser = "alice";

        // Create topic
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();
        // Create user
        KafkaUserResource.scramShaUser(CLUSTER_NAME, weirdUserName).done();
        KafkaUserResource.scramShaUser(CLUSTER_NAME, aliceUser).done();

        JsonObject config = new JsonObject();
        config.put("name", aliceUser);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        JsonObject response = BridgeUtils.createBridgeConsumer(config, bridgeHost, bridgePort, CONSUMER_GROUP_NAME, client);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(aliceUser));

        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(TOPIC_NAME);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);

        // Subscribe
        assertThat(BridgeUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, CONSUMER_GROUP_NAME, aliceUser, client), is(true));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withKafkaUsername(weirdUserName)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));
        // Try to consume messages
        JsonArray bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, CONSUMER_GROUP_NAME, aliceUser, client);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, CONSUMER_GROUP_NAME, aliceUser, client);
        }
        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(MESSAGE_COUNT));
        // Delete consumer
        assertThat(BridgeUtils.deleteConsumer(bridgeHost, bridgePort, CONSUMER_GROUP_NAME, aliceUser, client), is(true));
    }

    @BeforeAll
    void setup() throws InterruptedException {
        LOGGER.info("Deploy Kafka and KafkaBridge before tests");

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endKafkaListenerExternalNodePort()
                        .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        // Create Kafka user
        KafkaUserResource.scramShaUser(CLUSTER_NAME, USER_NAME).done();

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(USER_NAME);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME));

        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
            .withNewKafkaClientAuthenticationScramSha512()
                .withNewUsername(USER_NAME)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec().done();

        Service service = KafkaBridgeUtils.createBridgeNodePortService(CLUSTER_NAME, NAMESPACE, bridgeExternalService);
        KubernetesResource.createServiceResource(service, NAMESPACE).done();
        ServiceUtils.waitForNodePortService(bridgeExternalService);

        bridgePort = KafkaBridgeUtils.getBridgeNodePort(NAMESPACE, bridgeExternalService);
        bridgeHost = kubeClient(NAMESPACE).getNodeAddress();
    }
}
