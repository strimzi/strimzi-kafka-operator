/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.restassured.RestAssured.given;

public class HttpUtils {

    private static final Logger LOGGER = LogManager.getLogger(HttpUtils.class);

    private HttpUtils() { }

    public static void waitUntilServiceWithNameIsReady(String baserURI, String serviceName) {
        LOGGER.info("Wait until Service name {} is present in json", serviceName);
        TestUtils.waitFor("Service name " + serviceName + " is present in json", Constants.GLOBAL_TRACING_POLL, Constants.GLOBAL_TRACING_TIMEOUT,
            () -> {
                Response response = given()
                        .when()
                            .baseUri(baserURI)
                            .relaxedHTTPSValidation()
                            .contentType("application/json")
                            .get("/jaeger/api/services");

                return response.body().peek().print().contains(serviceName);
            });
        LOGGER.info("Service name {} is present", serviceName);
    }

    public static void waitUntilServiceWithNameIsReady(String baserURI, String... serviceNames) {
        for (String serviceName : serviceNames) {
            waitUntilServiceWithNameIsReady(baserURI, serviceName);
        }
    }

    public static void waitUntilServiceHasSomeTraces(String baseURI, String serviceName) {
        LOGGER.info("Wait untill Service {} has some traces", serviceName);
        TestUtils.waitFor("Service " + serviceName + " has some traces", Constants.GLOBAL_TRACING_POLL, Constants.GLOBAL_TRACING_TIMEOUT,
            () -> {
                Response response = given()
                            .when()
                                .baseUri(baseURI)
                                .relaxedHTTPSValidation()
                                .contentType("application/json")
                                .get("/jaeger/api/traces?service=" + serviceName);

                JsonPath jsonPathValidator = response.jsonPath();
                Map<Object, Object> data = jsonPathValidator.getMap("$");
                return data.size() > 0;
            });
        LOGGER.info("Service {} has traces", serviceName);
    }

    public static void waitUntilServiceHasSomeTraces(String baseURI, String... serviceNames) {
        for (String serviceName : serviceNames) {
            waitUntilServiceHasSomeTraces(baseURI, serviceName);
        }
    }

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
                                String value = jsonResponse.getString("value");
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

    public static boolean subscribeHttpConsumer(JsonObject topics, String bridgeHost, int bridgePort, String groupId, String name, WebClient client) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost,  "/consumers/" + groupId + "/instances/" + name + "/subscription")
                .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
                .putHeader("Content-type", Constants.KAFKA_BRIDGE_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(topics, ar -> {
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
}
