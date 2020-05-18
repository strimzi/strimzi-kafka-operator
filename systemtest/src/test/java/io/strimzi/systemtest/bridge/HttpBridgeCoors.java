/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HttpBridgeCoors extends HttpBridgeBaseST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeCoors.class);
    public static final String NAMESPACE = "bridge-cluster-test";
    private static final String COORS_ORIGIN = "https://strimzi.io";

    protected static String bridgeExternalService = CLUSTER_NAME + "-bridge-external-service";
    private static String bridgeHost;
    private static int bridgePort;

    @Test
    void testCorsForbidden() throws InterruptedException, ExecutionException, TimeoutException {

        final String kafkaBridgeUser = "bridge-user-example";
        final String topicName = "topic-simple-receive";
        final String groupId = "my-group-" + new Random().nextInt(Integer.MAX_VALUE);

        JsonObject config = new JsonObject();
        config.put("name", kafkaBridgeUser);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> coorsHeaders = new HashMap<>();
        coorsHeaders.put("Origin", "https://evil.io");
        coorsHeaders.put("Access-Control-Request-Method", "POST");

        BridgeUtils.createBridgeConsumer(config, bridgeHost, bridgePort, groupId, client, coorsHeaders);

        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(topicName);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        client.post(bridgePort, bridgeHost,  "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription")
            .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
            .putHeader("Content-type", Constants.KAFKA_BRIDGE_JSON)
            .putHeader("Origin", "https://evil.io")
            .putHeader("Access-Control-Request-Method", "POST")
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
        future.get(30, TimeUnit.SECONDS);
//        BridgeUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, kafkaBridgeUser, client, coorsHeaders);
//
//
//
//        // Subscribe
//        assertThat(BridgeUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, kafkaBridgeUser, client, coorsHeaders), is(false));
    }

    @BeforeAll
    static void beforeAll() throws InterruptedException {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1,1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaBridgeResource.kafkaBridgeWithCoors(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME),
            1, COORS_ORIGIN, null).done();

//e        KafkaBridgeHttpCors kafkaBridgeHttpCors = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getHttp().getCors();
//        LOGGER.info("Bridge with the following COORS settings {}", kafkaBridgeHttpCors.toString());

        Service service = KafkaBridgeUtils.createBridgeNodePortService(CLUSTER_NAME, NAMESPACE, bridgeExternalService);
        KubernetesResource.createServiceResource(service, NAMESPACE).done();
        ServiceUtils.waitForNodePortService(bridgeExternalService);

        bridgePort = KafkaBridgeUtils.getBridgeNodePort(NAMESPACE, bridgeExternalService);
        bridgeHost = kubeClient(NAMESPACE).getNodeAddress();
    }
}
