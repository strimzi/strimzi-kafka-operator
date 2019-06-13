/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

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
public class HttpBridgeST extends HttpBridgeBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);
    public static final String NAMESPACE = "bridge-cluster-test";
    private String bridgeHost = "";

    @Test
    void testSendSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-send";
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

        receiveMessagesConsole(NAMESPACE, topicName, messageCount);
    }

    @Test
    void testReceiveSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-receive";
        // Create topic
        testClassResources.topic(CLUSTER_NAME, topicName).done();

        String name = "my-kafka-consumer";
        String groupId = "my-group";

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");
        config.put("auto.offset.reset", "earliest");
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
        // Send messages to Kafka
        sendMessagesConsole(NAMESPACE, topicName, messageCount);
        // Try to consume messages
        JsonArray bridgeResponse = receiveHttpRequests(bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = receiveHttpRequests(bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name);
        }
        assertThat("Sent messages are equals", bridgeResponse.size(), is(messageCount));
        // Delete consumer
        assertTrue(deleteConsumer(bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name));
    }

    @BeforeAll
    void createClassResources() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
        // Deploy kafka
        testClassResources.kafkaEphemeral(CLUSTER_NAME, 1, 1)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalLoadBalancer()
                .withTls(false)
                .endKafkaListenerExternalLoadBalancer()
                .endListeners()
                .endKafka()
                .endSpec().done();

        Map<String, String> map = new HashMap<>();
        map.put("strimzi.io/cluster", CLUSTER_NAME);
        map.put("strimzi.io/kind", "KafkaBridge");
        map.put("strimzi.io/name", CLUSTER_NAME + "-bridge");

        // Deploy http bridge
        testClassResources.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT).done();
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
