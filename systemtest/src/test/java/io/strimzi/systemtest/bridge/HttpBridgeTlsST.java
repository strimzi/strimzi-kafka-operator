/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.HttpBridgeBaseST;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(BRIDGE)
@Tag(REGRESSION)
@ExtendWith(VertxExtension.class)
class HttpBridgeTlsST extends HttpBridgeBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeTlsST.class);
    private static final String NAMESPACE = "bridge-cluster-test-tls";

    private String bridgeHost = "";
    private int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;
    private String userName = "bob";

    @Test
    void testSendSimpleMessageTls() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-send";
        // Create topic
        getTestClassResources().topic(CLUSTER_NAME, topicName).done();

        JsonObject records = generateHttpMessages(messageCount);
        JsonObject response = sendHttpRequests(records, bridgeHost, bridgePort, topicName);
        checkSendResponse(response, messageCount);
        receiveMessagesExternalTls(NAMESPACE, topicName, messageCount, userName);
    }

    @Test
    void testReceiveSimpleMessageTls() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-receive";
        // Create topic
        getTestClassResources().topic(CLUSTER_NAME, topicName).done();

        String name = "my-kafka-consumer";
        String groupId = "my-group-" + new Random().nextInt(Integer.MAX_VALUE);

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create consumer
        JsonObject response = createBridgeConsumer(config, bridgeHost, bridgePort, groupId);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(name));
        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(topicName);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);
        // Subscribe
        assertTrue(subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, name));
        // Send messages to Kafka
        sendMessagesExternalTls(NAMESPACE, topicName, messageCount, userName);
        // Try to consume messages
        JsonArray bridgeResponse = receiveHttpRequests(bridgeHost, bridgePort, groupId, name);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = receiveHttpRequests(bridgeHost, bridgePort, groupId, name);
        }
        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(messageCount));
        // Delete consumer
        assertTrue(deleteConsumer(bridgeHost, bridgePort, groupId, name));
    }

    @BeforeAll
    void createClassResources() throws InterruptedException {
        LOGGER.info("Deploy Kafka and Kafka Bridge before tests");

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy kafka
        getTestClassResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalNodePort()
                .withTls(true)
                .endKafkaListenerExternalNodePort()
                .withTls(listenerTls)
                .withNewTls()
                .endTls()
                .endListeners()
                .endKafka()
                .endSpec().done();

        // Create Kafka user
        KafkaUser userSource = getTestClassResources().tlsUser(CLUSTER_NAME, userName).done();
        waitTillSecretExists(userName);

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(clusterCaCertSecretName(CLUSTER_NAME));

        // Deploy http bridge
        getTestClassResources().kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .editSpec()
            .withNewTls()
            .withTrustedCertificates(certSecret)
            .endTls()
            .endSpec().done();

        deployBridgeNodePortService();

        bridgePort = getBridgeNodePort();
        bridgeHost = kubeClient(NAMESPACE).getNodeAddress();
    }

    @Override
    public String getBridgeNamespace() {
        return NAMESPACE;
    }
}
