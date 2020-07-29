/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeHttpCors;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.hasItem;

public class HttpBridgeCorsST extends HttpBridgeAbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeCorsST.class);
    private static final String CORS_ORIGIN = "https://strimzi.io";

    @Test
    void testCorsOriginAllowed(VertxTestContext context) {
        final String kafkaBridgeUser = "bridge-user-example";
        final String topicName = "topic-simple-receive";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        JsonObject config = new JsonObject();
        config.put("name", kafkaBridgeUser);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(topicName);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);

        client.request(HttpMethod.OPTIONS, bridgePort, bridgeHost, "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription")
            .putHeader("Origin", CORS_ORIGIN)
            .putHeader("Access-Control-Request-Method", "POST")
            .putHeader("Content-length", String.valueOf(topics.toBuffer().length()))
            .putHeader("Content-type", Constants.KAFKA_BRIDGE_JSON)
            .sendJsonObject(config, ar -> context.verify(() -> {
                assertThat(ar.result().statusCode(), is(200));
                assertThat(ar.result().getHeader("access-control-allow-origin"), is(CORS_ORIGIN));
                assertThat(ar.result().getHeader("access-control-allow-headers"), is("access-control-allow-origin,origin,x-requested-with,content-type,access-control-allow-methods,accept"));
                List<String> list = Arrays.asList(ar.result().getHeader("access-control-allow-methods").split(","));
                assertThat(list, hasItem("POST"));
                client.request(HttpMethod.POST, bridgePort, bridgeHost, "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription")
                    .putHeader("Origin", CORS_ORIGIN)
                    .send(ar2 -> context.verify(() -> {
                        assertThat(ar2.result().statusCode(), is(404));
                        context.completeNow();
                    }));
            }));
    }

    @Test
    void testCorsForbidden(VertxTestContext context) {
        final String kafkaBridgeUser = "bridge-user-example";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        final String notAllowedOrigin = "https://evil.io";

        client.request(HttpMethod.OPTIONS, bridgePort, bridgeHost, "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription")
            .putHeader("Origin", notAllowedOrigin)
            .putHeader("Access-Control-Request-Method", "POST")
            .send(ar -> context.verify(() -> {
                assertThat(ar.result().statusCode(), is(403));
                assertThat(ar.result().statusMessage(), is("CORS Rejected - Invalid origin"));
                client.request(HttpMethod.POST, bridgePort, bridgeHost, "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription")
                    .putHeader("Origin", notAllowedOrigin)
                    .send(ar2 -> context.verify(() -> {
                        assertThat(ar2.result().statusCode(), is(403));
                        assertThat(ar2.result().statusMessage(), is("CORS Rejected - Invalid origin"));
                        context.completeNow();
                    }));
            }));
    }

    @BeforeAll
    static void beforeAll() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();

        KafkaBridgeResource.kafkaBridgeWithCors(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME),
            1, CORS_ORIGIN, null).done();

        KafkaBridgeHttpCors kafkaBridgeHttpCors = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getHttp().getCors();
        LOGGER.info("Bridge with the following CORS settings {}", kafkaBridgeHttpCors.toString());
    }
}
