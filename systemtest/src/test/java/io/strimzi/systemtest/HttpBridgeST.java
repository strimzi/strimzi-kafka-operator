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
import java.util.concurrent.TimeUnit;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Resources.getSystemtestsServiceResource;

@Tag(BRIDGE)
@Tag(REGRESSION)
@ExtendWith(VertxExtension.class)
public class HttpBridgeST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);
    public static final String NAMESPACE = "bridge-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private WebClient client;

    private String bridgeLoadBalancer = CLUSTER_NAME + "-loadbalancer";

    @Test
    void testHttpBridgeSendSimpleMessage() throws Exception {
        int messageCount = 50;
        JsonObject records = generateHttpMessages(messageCount);
        String bridgeHost = CLUSTER.client().getClient().services().withName(bridgeLoadBalancer).get().getSpec().getExternalIPs().get(0);

        CompletableFuture<Boolean> responsePromise = new CompletableFuture<>();
        sendHttpRequests(responsePromise, records, bridgeHost);
        responsePromise.get(60_000, TimeUnit.SECONDS);

        receiveMessages(messageCount, Constants.TIMEOUT_RECV_MESSAGES, CLUSTER_NAME, false, TOPIC_NAME, null);
    }

    @Test
    void testHttpBridgeReceiveSimpleMessage() throws Exception {
        int messageCount = 50;
        String bridgeHost = CLUSTER.client().getClient().services().inNamespace(NAMESPACE).withName(bridgeLoadBalancer).get().getSpec().getExternalIPs().get(0);
        String name = "my-kafka-consumer";
        String groupId = "my-group";

        String baseUri = "http://" + bridgeHost + ":" + Constants.HTTP_BRIDGE_DEFAULT_PORT + "/consumers/" + groupId + "/instances/" + name;

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");

        CompletableFuture<Boolean> createConsumer = new CompletableFuture<>();
        createBridgeConsumer(createConsumer, config, bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId);
        createConsumer.get(60_000, TimeUnit.SECONDS);

        JsonArray topic = new JsonArray();
        topic.add(TOPIC_NAME);

        JsonObject topics = new JsonObject();
        topics.put("topics", topic);

        CompletableFuture<Boolean> subscribeConsumer = new CompletableFuture<>();
        subscribHttpConsumer(subscribeConsumer, topics, bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name);
        subscribeConsumer.get(60_000, TimeUnit.SECONDS);

        sendMessages(messageCount, Constants.TIMEOUT_RECV_MESSAGES, CLUSTER_NAME, false, TOPIC_NAME, null);

        CompletableFuture<Boolean> consumerData = new CompletableFuture<>();
        receiveHttpRequests(consumerData, bridgeHost, Constants.HTTP_BRIDGE_DEFAULT_PORT, groupId, name);
        consumerData.get(60_000, TimeUnit.SECONDS);


    }

    @BeforeAll
    void createClassResources(Vertx vertx) throws Exception {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
        // Deploy kafka
        testClassResources.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
        // Deploy kafka clients inside openshift
        Service service = getSystemtestsServiceResource(Constants.KAFKA_CLIENTS, Environment.KAFKA_CLIENTS_DEFAULT_PORT).build();
        testClassResources.createServiceResource(service, NAMESPACE).done();
        testClassResources.createIngress(Constants.KAFKA_CLIENTS, Environment.KAFKA_CLIENTS_DEFAULT_PORT, CONFIG.getMasterUrl(), NAMESPACE).done();
        testClassResources.deployKafkaClients(CLUSTER_NAME, NAMESPACE).done();

        Map<String, String> map = new HashMap<>();
        map.put("strimzi.io/cluster", CLUSTER_NAME);
        map.put("strimzi.io/kind", "KafkaBridge");
        map.put("strimzi.io/name", CLUSTER_NAME + "-bridge");

        // Deploy http bridge
        testClassResources.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT).done();
        // Create load balancer service for expose bridge outside openshift
        service = getSystemtestsServiceResource(bridgeLoadBalancer, Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .editSpec()
                .withType("LoadBalancer")
                .withSelector(map)
                .endSpec().build();
        testClassResources.createServiceResource(service, NAMESPACE).done();
        // Create topic
        testClassResources.topic(CLUSTER_NAME, TOPIC_NAME).done();
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
            String msg = "message-" + i;
            json.put("value", msg);
            records.add(json);
        }
        JsonObject root = new JsonObject();
        root.put("records", records);
        return root;
    }

    void sendHttpRequests(CompletableFuture<Boolean> future, JsonObject records, String bridgeHost) {
        LOGGER.info("Sending records to Kafka Bridge");
        client.post(Constants.HTTP_BRIDGE_DEFAULT_PORT, bridgeHost, "/topics/" + TOPIC_NAME)
                .putHeader("Content-length", String.valueOf(records.toBuffer().length()))
                .putHeader("Content-Type", Constants.KAFKA_BRIDGE_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(records, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.debug("Server accepted post");
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        LOGGER.debug("offset size: {}", offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        LOGGER.debug("partition: {}", metadata.getInteger("partition"));
                        LOGGER.debug("offset size: {}", metadata.getLong("offset"));
                        future.complete(true);
                    } else {
                        LOGGER.debug("Server didn't accept post");
                        future.completeExceptionally(ar.cause());
                    }
                });
    }

    void receiveHttpRequests(CompletableFuture<Boolean> future, String bridgeHost, int bridgePort, String groupID, String name) {
        client.get(bridgePort, bridgeHost,"/consumers/" + groupID + "/instances/" + name + "/records?timeout=" + 1000)
                .putHeader("Accept", Constants.KAFKA_BRIDGE_JSON_JSON)
                .as(BodyCodec.jsonArray())
                .send(ar -> {
                    if (ar.succeeded() && ar.result().statusCode() == 200) {
                        LOGGER.info("Consuming messages...");
                        HttpResponse<JsonArray> response = ar.result();
                        JsonObject jsonResponse = response.body().getJsonObject(0);

                        String kafkaTopic = jsonResponse.getString("topic");
                        int kafkaPartition = jsonResponse.getInteger("partition");
                        String key = jsonResponse.getString("key");
                        String value = jsonResponse.getString("value");
                        long offset = jsonResponse.getLong("offset");
                        LOGGER.info("Received msg: topic:{} partition:{} key:{} value:{} offset{}", kafkaTopic, kafkaPartition, key, value, offset);

                    } else {
                        LOGGER.info("Cannot consume any messages!");
                        future.completeExceptionally(ar.cause());
                    }
                });
    }

    void subscribHttpConsumer(CompletableFuture<Boolean> future, JsonObject topics, String bridgeHost, int bridgePort, String groupID, String name) {
        client.post(bridgePort , bridgeHost, "/consumers/" + groupID + "/instances/" + name + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-type", Constants.KAFKA_BRIDGE_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
                    if (ar.succeeded() && ar.result().statusCode() == 204) {
                        LOGGER.info("Subscribed");
                        future.complete(true);
                    } else {
                        LOGGER.info("Cannot subscribe consumer!");
                        future.completeExceptionally(ar.cause());
                    }
                });

    }

    void createBridgeConsumer(CompletableFuture<Boolean> future, JsonObject config, String bridgeHost, int bridgePort, String groupId) {
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
                        LOGGER.info("ConsumerInstanceId: {}", consumerInstanceId);
                        LOGGER.info("ConsumerBaseUri: {}", consumerBaseUri);
                        future.complete(true);
                    } else {
                        LOGGER.info("Cannot create consumer!");
                        future.completeExceptionally(ar.cause());
                    }
                });
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }
}
