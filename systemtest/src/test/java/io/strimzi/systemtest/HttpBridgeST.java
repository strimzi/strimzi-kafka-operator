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

        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();
        sendHttpRequests(responsePromise, records, bridgeHost);
        responsePromise.get(60_000, TimeUnit.SECONDS);

        receiveMessages(messageCount, Constants.TIMEOUT_RECV_MESSAGES, CLUSTER_NAME, false, TOPIC_NAME, null);
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
                .editPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .endPort()
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

    String getBootstrapServer() {
        return CLUSTER_NAME + "-kafka-bootstrap:9092";
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

    void sendHttpRequests(CompletableFuture future, JsonObject records, String bridgeHost) {
        LOGGER.info("Sending records to Kafka Bridge");
        client.post(Constants.HTTP_BRIDGE_DEFAULT_PORT, bridgeHost, "/topics/" + TOPIC_NAME)
                .putHeader("Content-length", String.valueOf(records.toBuffer().length()))
                .putHeader("Content-Type", "application/vnd.kafka.json.v2+json")
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
                        future.complete(ar.result().body());
                    } else {
                        LOGGER.debug("Server didn't accept post");
                        future.completeExceptionally(ar.cause());
                    }
                });
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }
}
