/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BridgeUtils {

    private static final Logger LOGGER = LogManager.getLogger(HttpUtils.class);

    private BridgeUtils() { }

    public static JsonObject generateHttpMessages(int messageCount) {
        LOGGER.info("Creating {} records for KafkaBridge", messageCount);
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

    public static JsonObject sendMessagesHttpRequest(JsonObject records, String bridgeHost, int bridgePort, String topicName, WebClient client) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Sending records to KafkaBridge");
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost, "/topics/" + topicName)
            .putHeader("Content-length", String.valueOf(records.toBuffer().length()))
            .putHeader("Content-Type", Constants.KAFKA_BRIDGE_JSON_JSON)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(records, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<JsonObject> response = ar.result();
                    if (response.statusCode() == HttpResponseStatus.OK.code()) {
                        LOGGER.debug("Server accepted post");
                        future.complete(response.body());
                    } else {
                        LOGGER.error("Server didn't accept post", ar.cause());
                    }
                } else {
                    LOGGER.error("Server didn't accept post", ar.cause());
                    future.completeExceptionally(ar.cause());
                }
            });
        return future.get(1, TimeUnit.MINUTES);
    }

    public static JsonArray receiveMessagesHttpRequest(String bridgeHost, int bridgePort, String groupID, String name, WebClient client) throws Exception {
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
                            LOGGER.info("JsonResponse: {}", jsonResponse.toString());
                            String kafkaTopic = jsonResponse.getString("topic");
                            int kafkaPartition = jsonResponse.getInteger("partition");
                            String key = jsonResponse.getString("key");
                            Object value = jsonResponse.getValue("value");
                            long offset = jsonResponse.getLong("offset");
                            LOGGER.debug("Received msg: topic:{} partition:{} key:{} value:{} offset{}", kafkaTopic, kafkaPartition, key, value, offset);
                        }
                        LOGGER.info("Received {} messages from KafkaBridge", response.body().size());
                    } else {
                        LOGGER.warn("Received body 0 messages: {}", response.body());
                    }
                    future.complete(response.body());
                } else {
                    LOGGER.info("Cannot consume any messages!", ar.cause());
                    future.completeExceptionally(ar.cause());
                }
            });
        return future.get(1, TimeUnit.MINUTES);
    }

    public static boolean subscribeHttpConsumer(JsonObject topics, String bridgeHost, int bridgePort, String groupId,
                                                String name, WebClient client, Map<String, String> additionalHeaders) throws InterruptedException, ExecutionException, TimeoutException {

        MultiMap headers = MultiMap.caseInsensitiveMultiMap()
            .add("Content-length", String.valueOf(topics.toBuffer().length()))
            .add("Content-type", Constants.KAFKA_BRIDGE_JSON);

        for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
            LOGGER.info("Adding header {} -> {}", header.getKey(), header.getValue());
            headers.add(header.getKey(), header.getValue());
        }

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        client.post(bridgePort, bridgeHost,  "/consumers/" + groupId + "/instances/" + name + "/subscription")
            .putHeaders(headers)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(topics, ar -> {
                LOGGER.info(ar.result());

                if (ar.succeeded() && ar.result().statusCode() == 204) {
                    LOGGER.info("Consumer subscribed");
                    future.complete(ar.succeeded());
                } else {
                    LOGGER.error("Cannot subscribe consumer", ar.cause());
                    future.completeExceptionally(ar.cause());
                }
            });
        return future.get(1, TimeUnit.MINUTES);
    }

    public static boolean subscribeHttpConsumer(JsonObject topics, String bridgeHost, int bridgePort, String groupId,
                                                String name, WebClient client) throws InterruptedException, ExecutionException, TimeoutException {
        return subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, name, client, Collections.emptyMap());
    }

    public static JsonObject createBridgeConsumer(JsonObject config, String bridgeHost, int bridgePort, String groupId,
                                                  WebClient client, Map<String, String> additionalHeaders) throws InterruptedException, ExecutionException, TimeoutException {

        MultiMap headers = MultiMap.caseInsensitiveMultiMap()
            .add("Content-length", String.valueOf(config.toBuffer().length()))
            .add("Content-type", Constants.KAFKA_BRIDGE_JSON);

        for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
            LOGGER.info("Adding header {} -> {}", header.getKey(), header.getValue());
            headers.add(header.getKey(), header.getValue());
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost, "/consumers/" + groupId)
            .putHeaders(headers)
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
                    LOGGER.error("Cannot create consumer", ar.cause());
                    future.completeExceptionally(ar.cause());
                }
            });
        return future.get(1, TimeUnit.MINUTES);
    }

    public static JsonObject createBridgeConsumer(JsonObject config, String bridgeHost, int bridgePort, String groupId,
                                                  WebClient webClient) throws InterruptedException, ExecutionException, TimeoutException {
        return createBridgeConsumer(config, bridgeHost, bridgePort, groupId, webClient, Collections.emptyMap());
    }

    public static boolean deleteConsumer(String bridgeHost, int bridgePort, String groupId, String name, WebClient client) throws InterruptedException, ExecutionException, TimeoutException {
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
                    LOGGER.error("Cannot delete consumer", ar.cause());
                    future.completeExceptionally(ar.cause());
                }
            });
        return future.get(1, TimeUnit.MINUTES);
    }

    /**
     * Returns Strimzi Kafka Bridge version which is associated with Strimzi Kafka Operator.
     * The value is parsed from {@code /bridge.version} classpath resource and return it as a string.
     * @return bridge version
     */
    public static String getBridgeVersion() {
        InputStream bridgeVersionInputStream = BridgeUtils.class.getResourceAsStream("/bridge.version");
        return TestUtils.readResource(bridgeVersionInputStream).replace("\n", "");
    }
}
