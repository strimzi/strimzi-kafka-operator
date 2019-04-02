/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import io.strimzi.systemtest.VertxFactory;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

public class KafkaClient implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClient.class);
    private final Map<String, Vertx> clients = new HashMap<>();

    public KafkaClient() {
    }

    @Override
    public void close() {
        for (Map.Entry<String, Vertx> client : clients.entrySet()) {
            client.getValue().close();
        }
    }

    /**
     * Send messages to external entrypoint of the cluster with PLAINTEXT security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param messageCount message count
     * @return future with sent message count
     */
    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount) {
        String clientName = "sender-plain-" + clusterName;
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.put(clientName, vertx);

        IntPredicate msgCntPredicate = (x) -> x == messageCount;

        vertx.deployVerticle(new Producer(KafkaClientProperties.createProducerProperties(namespace, clusterName), resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }

    /**
     * Send messages to external entrypoint of the cluster with SSL security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param userName user name for authorization
     * @param messageCount message count
     * @return future with sent message count
     */
    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String userName, int messageCount) {
        String clientName = "sender-ssl" + clusterName;
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.put(clientName, vertx);

        IntPredicate msgCntPredicate = (x) -> x == messageCount;

        vertx.deployVerticle(new Producer(KafkaClientProperties.createProducerProperties(namespace, clusterName, userName, "SSL"), resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }

    /**
     *
     * @param topicName
     * @param namespace
     * @param clusterName
     * @param userName
     * @param clientName
     * @return
     */
    public CompletableFuture<Integer> sendMessagesUntilNotification(String topicName, String namespace, String clusterName, String userName, String clientName) {
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.put(clientName, vertx);

        IntPredicate msgCntPredicate = (x) -> x == -1;

        vertx.deployVerticle(new Producer(KafkaClientProperties.createProducerProperties(namespace, clusterName, userName, "SSL"), resultPromise, msgCntPredicate, topicName, clientName));

        return resultPromise;
    }

    /**
     * Receive messages to external entrypoint of the cluster with PLAINTEXT security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param messageCount message count
     * @return future with received message count
     */
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount) {
        String clientName = "receiver-plain-" + clusterName;
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.put(clientName, vertx);

        IntPredicate msgCntPredicate = (x) -> x == messageCount;

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createConsumerProperties(namespace, clusterName), resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }

    /**
     * Receive messages to external entrypoint of the cluster with SSL security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param userName user name for authorization
     * @param messageCount message count
     * @return future with received message count
     */
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String userName, int messageCount) {
        String clientName = "receiver-plain-" + clusterName;
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.put(clientName, vertx);

        IntPredicate msgCntPredicate = (x) -> x == messageCount;

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createConsumerProperties(namespace, clusterName, userName, "SSL"), resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }

    /**
     *
     * @param topicName
     * @param namespace
     * @param clusterName
     * @param userName
     * @param clientName
     * @return
     */
    public CompletableFuture<Integer> receiveMessagesUntilNotification(String topicName, String namespace, String clusterName, String userName, String clientName) {
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.put(clientName, vertx);

        IntPredicate msgCntPredicate = (x) -> x == -1;

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createConsumerProperties(namespace, clusterName, userName, "SSL"), resultPromise, msgCntPredicate, topicName, clientName));

        return resultPromise;
    }

    public void sendNotificationToClient(String clientName, String notification) {
        Vertx vertx = clients.get(clientName);
        LOGGER.info("Sending {} to {}", notification, clientName);
        vertx.eventBus().publish(clientName, notification);
    }
}
