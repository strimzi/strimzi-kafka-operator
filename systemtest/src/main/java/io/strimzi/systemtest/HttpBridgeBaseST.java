/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeBaseST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeBaseST.class);
    private WebClient client;

    protected String bridgeLoadBalancer = CLUSTER_NAME + "-loadbalancer";

    @BeforeAll
    void prepareEnv(Vertx vertx) {
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
                            LOGGER.debug("Received 0 messages, going to consume again");
                        }
                        future.complete(response.body());
                    } else {
                        LOGGER.info("Cannot consume any messages!");
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    boolean subscribeHttpConsumer(JsonObject topics, String bridgeHost, int bridgePort, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
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
