/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaResources;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
public class HttpBridgeST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);
    public static final String NAMESPACE = "bridge-cluster-test";
    private WebClient client;

    private String bridgeLoadBalancer = CLUSTER_NAME + "-loadbalancer";

    @Test
    void testHttpBridgeSendSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-send";
        // Create topic
        testClassResources.topic(CLUSTER_NAME, topicName).done();

        JsonObject records = generateHttpMessages(messageCount);
        String bridgeHost = CLUSTER.client().getClient().services().inNamespace(NAMESPACE).withName(bridgeLoadBalancer).get().getSpec().getExternalIPs().get(0);

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
    void testHttpBridgeReceiveSimpleMessage() throws Exception {
        int messageCount = 50;
        String topicName = "topic-simple-receive";
        // Create topic
        testClassResources.topic(CLUSTER_NAME, topicName).done();

        String bridgeHost = CLUSTER.client().getClient().services().inNamespace(NAMESPACE).withName(bridgeLoadBalancer).get().getSpec().getExternalIPs().get(0);
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
        assertTrue(subscribHttpConsumer(topics, bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name));
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
    void createClassResources(Vertx vertx) {
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
        // Create http client
        client = WebClient.create(vertx, new WebClientOptions()
                .setSsl(false)
                .setTrustAll(true)
                .setVerifyHost(false));
    }

    JsonObject generateHttpMessages(int messageCount) {
        LOGGER.info("Creating {} records for Kafka Bridge", messageCount);
        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        for (int i = 0; i < messageCount; i++) {
            json.put("value", "msg_" + i);
            records.add(json);
        }
        JsonObject root = new JsonObject();
        root.put("records", records);
        return root;
    }

    JsonObject sendHttpRequests(JsonObject records, String bridgeHost, String topic) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Sending records to Kafka Bridge");
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        client.post(Constants.HTTP_BRIDGE_DEFAULT_PORT, bridgeHost, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(records.toBuffer().length()))
                .putHeader("Content-Type", Constants.KAFKA_BRIDGE_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(records, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.debug("Server accepted post");
                        HttpResponse<JsonObject> response = ar.result();
                        future.complete(response.body());
                    } else {
                        LOGGER.error("Server didn't accept post: {}", ar.result());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    JsonArray receiveHttpRequests(String bridgeHost, int bridgePort, String groupID, String name) throws Exception {
        CompletableFuture<JsonArray> future = new CompletableFuture<>();
        client.get(bridgePort, bridgeHost, "/consumers/" + groupID + "/instances/" + name + "/records?timeout=" + 1000)
                .putHeader("Accept", Constants.KAFKA_BRIDGE_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    if (ar.succeeded() && ar.result().statusCode() == 200) {
                        HttpResponse<JsonArray> response = ar.result();
                        if (response.body().size() > 0) {
                            for (int i = 0; i < response.body().size(); i++) {
                                JsonObject jsonResponse = response.body().getJsonObject(i);
                                String kafkaTopic = jsonResponse.getString("topic");
                                int kafkaPartition = jsonResponse.getInteger("partition");
                                String key = jsonResponse.getString("key");
                                int value = jsonResponse.getInteger("value");
                                long offset = jsonResponse.getLong("offset");
                                LOGGER.debug("Received msg: topic:{} partition:{} key:{} value:{} offset{}", kafkaTopic, kafkaPartition, key, value, offset);
                            }
                        } else {
                            LOGGER.warn("Received 0 messages");
                        }
                        future.complete(response.body());
                    } else {
                        LOGGER.info("Cannot consume any messages!");
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    boolean subscribHttpConsumer(JsonObject topics, String bridgeHost, int bridgePort, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost,  "/consumers/" + groupId + "/instances/" + name + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-type", Constants.KAFKA_BRIDGE_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    if (ar.succeeded() && ar.result().statusCode() == 204) {
                        LOGGER.info("Subscribed");
                        future.complete(ar.succeeded());
                    } else {
                        LOGGER.error("Cannot subscribe consumer: {}", ar.result());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    JsonObject createBridgeConsumer(JsonObject config, String bridgeHost, int bridgePort, String groupId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost, "/consumers/" + groupId)
                .putHeader("Content-length", String.valueOf(config.toBuffer().length()))
                .putHeader("Content-type", Constants.KAFKA_BRIDGE_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(config, ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();
                        String consumerInstanceId = bridgeResponse.getString("instance_id");
                        String consumerBaseUri = bridgeResponse.getString("base_uri");
                        LOGGER.debug("ConsumerInstanceId: {}", consumerInstanceId);
                        LOGGER.debug("ConsumerBaseUri: {}", consumerBaseUri);
                        future.complete(response.body());
                    } else {
                        LOGGER.error("Cannot create consumer: {}", ar.result());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    boolean deleteConsumer(String bridgeHost, int bridgePort, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Deleting consumer");
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        client.delete(bridgePort, bridgeHost, "/consumers/" + groupId + "/instances/" + name)
                .putHeader("Content-Type", Constants.KAFKA_BRIDGE_JSON)
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("Consumer deleted");
                        future.complete(ar.succeeded());
                    } else {
                        LOGGER.error("Cannot delete consumer: {}", ar.result());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }
}
