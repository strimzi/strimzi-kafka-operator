/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Base for test classes where HTTP Bridge is used.
 */
@ExtendWith(VertxExtension.class)
@Tag(BRIDGE)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
public class HttpBridgeAbstractST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeAbstractST.class);

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

    protected void doSimpleReceiveMessages(String clusterName, String bridgeHost, int bridgePort, String namespace, String clientsPodName) throws Exception {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        // Create topic
        KafkaTopicResource.topic(clusterName, topicName).done();

        String name = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String groupId = "my-group-" + new Random().nextInt(Integer.MAX_VALUE);

        JsonObject config = new JsonObject();
        config.put("name", name);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create consumer
        JsonObject response = BridgeUtils.createBridgeConsumer(config, bridgeHost, bridgePort, groupId, client);
        assertThat("Consumer wasn't created correctly", response.getString("instance_id"), is(name));
        // Create topics json
        JsonArray topic = new JsonArray();
        topic.add(topicName);
        JsonObject topics = new JsonObject();
        topics.put("topics", topic);
        // Subscribe
        assertThat(BridgeUtils.subscribeHttpConsumer(topics, bridgeHost, bridgePort, groupId, name, client), is(true));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(clientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(namespace)
                .withClusterName(clusterName)
                .withMessageCount(MESSAGE_COUNT)
                .build();

        // Send messages to Kafka
        assertThat(internalKafkaClient.sendMessagesPlain(), is(MESSAGE_COUNT));

        // Try to consume messages
        JsonArray bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, groupId, name, client);
        if (bridgeResponse.size() == 0) {
            // Real consuming
            bridgeResponse = BridgeUtils.receiveMessagesHttpRequest(bridgeHost, bridgePort, groupId, name, client);
        }
        assertThat("Sent message count is not equal with received message count", bridgeResponse.size(), is(MESSAGE_COUNT));
        // Delete consumer
        assertThat(deleteConsumer(bridgeHost, bridgePort, groupId, name), is(true));
    }

    protected void doSimpleSendMessages(String clusterName, String bridgeHost, int bridgePort,
                                        String namespace, String clientsPodName) throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        // Create topic
        KafkaTopicResource.topic(clusterName, topicName).done();

        JsonObject records = BridgeUtils.generateHttpMessages(MESSAGE_COUNT);
        JsonObject response = BridgeUtils.sendMessagesHttpRequest(records, bridgeHost, bridgePort, topicName, client);
        KafkaBridgeUtils.checkSendResponse(response, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(clientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(namespace)
                .withClusterName(clusterName)
                .withMessageCount(MESSAGE_COUNT)
                .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), is(MESSAGE_COUNT));
    }

    public String getBridgeNamespace() {
        return "bridge-cluster-test";
    }

    @BeforeAll
    void deployClusterOperator(Vertx vertx) throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(getBridgeNamespace());

        // Create http client
        client = WebClient.create(vertx, new WebClientOptions()
            .setSsl(false));
    }
}
