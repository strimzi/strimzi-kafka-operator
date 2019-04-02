/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import io.strimzi.systemtest.VertxFactory;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaClient implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClient.class);
    private final List<Vertx> clients = new ArrayList<>();

    public KafkaClient() {
    }

    @Override
    public void close() {
        for (Vertx client : clients) {
            client.close();
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
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.add(vertx);

        vertx.deployVerticle(new Producer(KafkaClientProperties.createProducerProperties(namespace, clusterName), resultPromise, messageCount, topicName));

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
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.add(vertx);

        vertx.deployVerticle(new Producer(KafkaClientProperties.createProducerProperties(namespace, clusterName, userName, "SSL"), resultPromise, messageCount, topicName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
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
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.add(vertx);

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createConsumerProperties(namespace, clusterName), resultPromise, messageCount, topicName));

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
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.add(vertx);

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createConsumerProperties(namespace, clusterName, userName, "SSL"), resultPromise, messageCount, topicName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }
}
