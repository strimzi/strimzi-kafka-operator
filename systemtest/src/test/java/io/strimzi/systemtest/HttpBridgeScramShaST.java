/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.systemtest.utils.StUtils;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Resources.getSystemtestsServiceResource;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(BRIDGE)
@Tag(REGRESSION)
@ExtendWith(VertxExtension.class)
public class HttpBridgeScramShaST extends HttpBridgeBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeScramShaST.class);
    public static final String NAMESPACE = "bridge-scram-sha-cluster-test";

    private String userName = "pepa";
    private String bridgeHost = "";

    @Test
    void testSendSimpleMessageTlsScramSha() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-send-" + new Random().nextInt(Integer.MAX_VALUE);
        // Create topic
        testClassResources.topic(CLUSTER_NAME, topicName).done();

        JsonObject records = generateHttpMessages(messageCount);
        JsonObject response = sendHttpRequests(records, bridgeHost, topicName);
        JsonArray offsets = response.getJsonArray("offsets");
        assertEquals(messageCount, offsets.size());
        for (int i = 0; i < messageCount; i++) {
            JsonObject metadata = offsets.getJsonObject(i);
            assertEquals(0, metadata.getInteger("partition"));
            assertEquals(i, metadata.getLong("offset"));
            LOGGER.debug("offset size: {}, partition: {}, offset size: {}", offsets.size(), metadata.getInteger("partition"), metadata.getLong("offset"));
        }

        receiveMessagesConsoleScramSha(NAMESPACE, topicName, messageCount, userName);
    }

    @Test
    void testReceiveSimpleMessageTlsScramSha() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-receive-" + new Random().nextInt(Integer.MAX_VALUE);
        // Create topic
        testClassResources.topic(CLUSTER_NAME, topicName).done();
        // Send messages to Kafka
        sendMessagesConsoleScramSha(NAMESPACE, topicName, messageCount, userName);

        String name = "kafka-consumer-simple-receive";
        String groupId = "my-group-" + new Random().nextInt(Integer.MAX_VALUE);

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create consumer
        JsonObject response = createBridgeConsumer(config, bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(name));
        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(topicName);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);
        // Subscribe
        assertTrue(subscribeHttpConsumer(topics, bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name));
        // Try to consume messages
        JsonArray bridgeResponse = receiveHttpRequests(bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = receiveHttpRequests(bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name);
        }

        assertThat("Sent messages are not equals", bridgeResponse.size(), is(messageCount));
        // Delete consumer
        assertTrue(deleteConsumer(bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name));
    }

    @BeforeAll
    void createClassResources() {
        LOGGER.info("Deploy Kafka and Kafka Bridge before tests");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy kafka
        testClassResources.kafkaEphemeral(CLUSTER_NAME, 1, 1)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withNewKafkaListenerExternalLoadBalancer()
                .withTls(true)
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .endKafkaListenerExternalLoadBalancer()
                .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                .endListeners()
                .endKafka()
                .endSpec().done();

        // Create Kafka user
        KafkaUser userSource = testClassResources.scramShaUser(CLUSTER_NAME, userName).done();
        waitTillSecretExists(userName);

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(userName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(clusterCaCertSecretName(CLUSTER_NAME));

        // Deploy http bridge
        testClassResources.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .editSpec()
            .withNewKafkaBridgeAuthenticationScramSha512()
                .withNewUsername(userName)
                .withPasswordSecret(passwordSecret)
            .endKafkaBridgeAuthenticationScramSha512()
                .withNewTls()
                .withTrustedCertificates(certSecret)
                .endTls()
            .endSpec().done();

        Map<String, String> map = new HashMap<>();
        map.put("strimzi.io/cluster", CLUSTER_NAME);
        map.put("strimzi.io/kind", "KafkaBridge");
        map.put("strimzi.io/name", CLUSTER_NAME + "-bridge");
        // Create load balancer service for expose bridge outside openshift
        Service service = getSystemtestsServiceResource(bridgeLoadBalancer, Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .editSpec()
                .withType("LoadBalancer")
                .withSelector(map)
                .endSpec().build();
        testClassResources.createServiceResource(service, NAMESPACE).done();
        StUtils.waitForLoadBalancerService(bridgeLoadBalancer);
        bridgeHost = CLUSTER.client().getClient().services().inNamespace(NAMESPACE).withName(bridgeLoadBalancer).get().getSpec().getExternalIPs().get(0);
    }
}
