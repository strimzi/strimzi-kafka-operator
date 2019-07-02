/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.systemtest.utils.StUtils;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Resources.getSystemtestsServiceResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeBaseST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeBaseST.class);
    private WebClient client;

    protected String bridgeExternalService = CLUSTER_NAME + "-bridge-external-service";

    @BeforeAll
    void prepareEnv(Vertx vertx) {
        // Create http client
        client = WebClient.create(vertx, new WebClientOptions()
                .setSsl(false));
    }

    protected JsonObject generateHttpMessages(int messageCount) {
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

    protected JsonObject sendHttpRequests(JsonObject records, String bridgeHost, int bridgePort, String topic) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Sending records to Kafka Bridge");
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        client.post(bridgePort, bridgeHost, "/topics/" + topic)
                .putHeader("Content-length", String.valueOf(records.toBuffer().length()))
                .putHeader("Content-Type", Constants.KAFKA_BRIDGE_JSON_JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(records, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.debug("Server accepted post");
                        HttpResponse<JsonObject> response = ar.result();
                        future.complete(response.body());
                    } else {
                        LOGGER.error("Server didn't accept post", ar.cause());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    protected JsonArray receiveHttpRequests(String bridgeHost, int bridgePort, String groupID, String name) throws Exception {
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
                            LOGGER.info("Received {} messages from the bridge", response.body().size());
                        } else {
                            LOGGER.debug("Received 0 messages, going to consume again");
                        }
                        future.complete(response.body());
                    } else {
                        LOGGER.info("Cannot consume any messages!", ar.cause());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    protected boolean subscribeHttpConsumer(JsonObject topics, String bridgeHost, int bridgePort, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
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
                        LOGGER.error("Cannot subscribe consumer", ar.cause());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    protected JsonObject createBridgeConsumer(JsonObject config, String bridgeHost, int bridgePort, String groupId) throws InterruptedException, ExecutionException, TimeoutException {
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
                        LOGGER.error("Cannot create consumer", ar.cause());
                        future.completeExceptionally(ar.cause());
                    }
                });
        return future.get(1, TimeUnit.MINUTES);
    }

    protected boolean deleteConsumer(String bridgeHost, int bridgePort, String groupId, String name) throws InterruptedException, ExecutionException, TimeoutException {
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

    protected void deployBridgeNodePortService() throws InterruptedException {
        Map<String, String> map = new HashMap<>();
        map.put("strimzi.io/cluster", CLUSTER_NAME);
        map.put("strimzi.io/kind", "KafkaBridge");
        map.put("strimzi.io/name", CLUSTER_NAME + "-bridge");

        // Create node port service for expose bridge outside openshift
        Service service = getSystemtestsServiceResource(bridgeExternalService, Constants.HTTP_BRIDGE_DEFAULT_PORT, getBridgeNamespace())
                .editSpec()
                .withType("NodePort")
                .withSelector(map)
                .endSpec().build();
        getTestClassResources().createServiceResource(service, getBridgeNamespace()).done();
        StUtils.waitForNodePortService(bridgeExternalService);
    }

    protected void checkSendResponse(JsonObject response, int messageCount) {
        JsonArray offsets = response.getJsonArray("offsets");
        assertEquals(messageCount, offsets.size());
        for (int i = 0; i < messageCount; i++) {
            JsonObject metadata = offsets.getJsonObject(i);
            assertEquals(0, metadata.getInteger("partition"));
            assertEquals(i, metadata.getLong("offset"));
            LOGGER.debug("offset size: {}, partition: {}, offset size: {}", offsets.size(), metadata.getInteger("partition"), metadata.getLong("offset"));
        }
    }

    protected int getBridgeNodePort() {
        Service extBootstrapService = kubeClient(getBridgeNamespace()).getClient().services()
                .inNamespace(getBridgeNamespace())
                .withName(bridgeExternalService)
                .get();

        return extBootstrapService.getSpec().getPorts().get(0).getNodePort();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skipping env recreation after each test - deployment should be same for whole test class!");
    }

    public String getBridgeNamespace() {
        return "bridge-cluster-test";
    }

    @BeforeAll
    void deployClusterOperator() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(getBridgeNamespace());

        createTestClassResources();
        applyRoleBindings(getBridgeNamespace());
        // 050-Deployment
        getTestClassResources().clusterOperator(getBridgeNamespace()).done();
    }
}
