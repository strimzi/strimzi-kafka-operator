/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Service;
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

import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Resources.getSystemtestsServiceResource;

@Tag(BRIDGE)
@ExtendWith(VertxExtension.class)
public class HttpBridgeST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeST.class);
    public static final String NAMESPACE = "bridge-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private String httpBridgeHost = "";


    @Test
    void testHttpBridgeSendSimpleMessage(Vertx vertx) throws Exception {
        String value = "message-value";
        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        json.put("value", value);
        records.add(json);

        JsonObject root = new JsonObject();
        root.put("records", records);

        WebClient client = WebClient.create(vertx, new WebClientOptions()
                .setSsl(false)
                .setTrustAll(true)
                .setVerifyHost(false));

        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();

        client.post(8080, "test-bridge-route-bridge-cluster-test.10.0.140.50.nip.io", "/topics/" + TOPIC_NAME)
                .putHeader("Content-length", String.valueOf(root.toBuffer().length()))
                .as(BodyCodec.jsonObject())
                .sendJsonObject(root, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("Server accepted post");
                        HttpResponse<JsonObject> response = ar.result();
                        JsonObject bridgeResponse = response.body();

                        JsonArray offsets = bridgeResponse.getJsonArray("offsets");
                        LOGGER.info("offset size: {}", offsets.size());
                        JsonObject metadata = offsets.getJsonObject(0);
                        LOGGER.info("partition: {}", metadata.getInteger("partition"));
                        LOGGER.info("offset size: {}", metadata.getLong("offset"));
                        responsePromise.complete(ar.result().body());
                    } else {
                        LOGGER.info("Server didn't accept post");
                        responsePromise.completeExceptionally(ar.cause());
                    }
                });

        responsePromise.get(20_000, TimeUnit.SECONDS);

        receiveMessages(1, 60000, CLUSTER_NAME, false, TOPIC_NAME, null);
    }

    @BeforeAll
    void createClassResources() throws Exception {
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
        // Deploy kafka clients inside openshift
        Service service = getSystemtestsServiceResource(Constants.KAFKA_CLIENTS, Environment.KAFKA_CLIENTS_DEFAULT_PORT);
        testClassResources.createServiceResource(service, NAMESPACE).done();
        testClassResources.createIngress(Constants.KAFKA_CLIENTS, Environment.KAFKA_CLIENTS_DEFAULT_PORT, CONFIG.getMasterUrl(), NAMESPACE).done();
        testClassResources.deployKafkaClients(CLUSTER_NAME, NAMESPACE).done();

        // Deploy http bridge
        testClassResources.kafkaBridge("test", getBootstrapServer(CLUSTER_NAME), 1, 8080).done();

        // Set bridge host
        String kubernetesAddress = Environment.KUBERNETES_DOMAIN.equals(Environment.KUBERNETES_DOMAIN_DEFAULT) ?
                new URL(CONFIG.getMasterUrl()).getHost() + Environment.KUBERNETES_DOMAIN_DEFAULT : Environment.KUBERNETES_DOMAIN;
        httpBridgeHost = "http://" + Constants.STRIMZI_BRIDGE_DEPLOYMENT_NAME + "." + kubernetesAddress;
        // Create topic
        testClassResources.topic(CLUSTER_NAME, TOPIC_NAME).done();
    }

    String getBootstrapServer(String clusterName) {
        return CLUSTER_NAME + "-kafka-bootstrap:9092";
    }

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        super.tearDownEnvironmentAfterEach();
    }

    @Override
    void tearDownEnvironmentAfterAll() {
        super.tearDownEnvironmentAfterAll();
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
    }
}
