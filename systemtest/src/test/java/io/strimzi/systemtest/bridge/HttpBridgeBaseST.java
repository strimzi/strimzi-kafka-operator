/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeBaseST extends BaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeBaseST.class);

    protected WebClient client;
    protected String bridgeExternalService = CLUSTER_NAME + "-bridge-external-service";

    @BeforeAll
    void prepareEnv(Vertx vertx) {
        // Create http client
        client = WebClient.create(vertx, new WebClientOptions()
                .setSsl(false));
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

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skipping env recreation after each test - deployment should be same for whole test class!");
    }

    public String getBridgeNamespace() {
        return "bridge-cluster-test";
    }

    @BeforeAll
    void deployClusterOperator() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(getBridgeNamespace());

        applyRoleBindings(getBridgeNamespace());
        // 050-Deployment
        KubernetesResource.clusterOperator(getBridgeNamespace()).done();
    }
}
