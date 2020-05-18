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
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base for test classes where HTTP Bridge is used.
 */
@ExtendWith(VertxExtension.class)
public class HttpBridgeBaseST extends BaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeBaseST.class);

    protected WebClient client;
    protected String bridgeExternalService = CLUSTER_NAME + "-bridge-external-service";

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
    void deployClusterOperator(Vertx vertx) {
        ResourceManager.setClassResources();
        prepareEnvForOperator(getBridgeNamespace());

        applyRoleBindings(getBridgeNamespace());
        // 050-Deployment
        KubernetesResource.clusterOperator(getBridgeNamespace()).done();

        // Create http client
        client = WebClient.create(vertx, new WebClientOptions()
            .setSsl(false));
    }
}
