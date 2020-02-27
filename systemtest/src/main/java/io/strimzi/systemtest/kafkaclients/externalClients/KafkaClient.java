/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.IKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

/**
 * The KafkaClient for sending and receiving messages with basic properties. The client is using an external listeners.
 */
public class KafkaClient implements AutoCloseable, IKafkaClient<Future<Integer>> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClient.class);
    private Vertx vertx;

    private String caCertName;

    @Override
    public void close() {
        if (vertx != null) {
            vertx.close();
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
    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount) throws IOException {
        return sendMessages(topicName, namespace, clusterName, messageCount, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }


    /**
     * Send messages to external entrypoint of the cluster with PLAINTEXT security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param messageCount message count
     * @return future with sent message count
     */
    @Override
    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount,
                                        long timeoutMs) throws IOException {
        String clientName = "sender-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        vertx.deployVerticle(new Producer(KafkaClientProperties.createBasicProducerProperties(namespace, clusterName),
                resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    /**
     * Send messages to external entrypoint of the cluster with SSL security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param kafkaUsername user name for authorization
     * @param messageCount message count
     * @param securityProtocol security protocol to encrypt communication
     * @return future with sent message count
     */
    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                           int messageCount, String securityProtocol) throws IOException {
        return sendMessagesTls(topicName, namespace, clusterName, kafkaUsername, messageCount, securityProtocol,
                Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Send messages to external entrypoint of the cluster with SSL security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param kafkaUsername user name for authorization
     * @param messageCount message count
     * @return future with sent message count
     */
    @Override
    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                           int messageCount, String securityProtocol, long timeoutMs) throws IOException {
        String clientName = "sender-ssl" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(namespace, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        vertx.deployVerticle(new Producer(KafkaClientProperties.createBasicProducerTlsProperties(namespace, clusterName,
                caCertName, kafkaUsername, securityProtocol),
                resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
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
    @Override
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                           String consumerGroup, long timeoutMs) throws IOException {
        String clientName = "receiver-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createBasicConsumerProperties(namespace, clusterName, consumerGroup),
                resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
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
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount) throws IOException {
        return receiveMessages(topicName, namespace, clusterName, messageCount,
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE), Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Receive messages to external entry-point of the cluster with SSL security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param userName user name for authorization
     * @param messageCount message count
     * @return future with received message count
     */
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String userName,
                                              int messageCount, String securityProtocol) throws IOException {
        return receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, securityProtocol,
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE), Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Receive messages to external entry-point of the cluster with SSL security protocol setting and specific consumer group
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param userName user name for authorization
     * @param messageCount message count
     * @param consumerGroup consumer group name
     * @return future with received message count
     */
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String userName,
                                              int messageCount, String securityProtocol, String consumerGroup) throws IOException {
        return receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, securityProtocol,
                consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Receive messages to external entrypoint of the cluster with SSL security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param kafkaUsername user name for authorization
     * @param messageCount message count
     * @return future with received message count
     */
    @Override
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                              int messageCount, String securityProtocol, String consumerGroup,
                                              long timeoutMs) throws IOException {
        String clientName = "receiver-ssl-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(namespace, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createBasicConsumerTlsProperties(namespace, clusterName,
                consumerGroup, caCertName, kafkaUsername, securityProtocol),
                resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public String getCaCertName() {
        return caCertName;
    }

    public void setCaCertName(String caCertName) {
        this.caCertName = caCertName;
    }
}
