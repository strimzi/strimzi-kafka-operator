/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.AbstractKafkaClient;
import io.strimzi.systemtest.kafkaclients.IKafkaClientOperations;
import io.strimzi.systemtest.kafkaclients.KafkaClientProperties;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

/**
 * The BasicExternalKafkaClient for sending and receiving messages with basic properties. The client is using an external listeners.
 */
public class BasicExternalKafkaClient extends AbstractKafkaClient implements AutoCloseable, IKafkaClientOperations<Future<Integer>> {

    private static final Logger LOGGER = LogManager.getLogger(BasicExternalKafkaClient.class);
    private Vertx vertx = Vertx.vertx();

    public static class Builder extends AbstractKafkaClient.Builder<Builder> {

        @Override
        public BasicExternalKafkaClient build() {
            return new BasicExternalKafkaClient(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

    private BasicExternalKafkaClient(Builder builder) {
        super(builder);
    }

    public Future<Integer> sendMessagesPlain() {
        return sendMessagesPlain(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Send messages to external entrypoint of the cluster with PLAINTEXT security protocol setting
     * @return future with sent message count
     */
    @Override
    public Future<Integer> sendMessagesPlain(long timeoutMs) {

        String clientName = "sender-plain-" + this.clusterName;
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        vertx.deployVerticle(new Producer(
            new KafkaClientProperties.KafkaClientPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
                .withKeySerializerConfig(StringSerializer.class.getName())
                .withValueSerializerConfig(StringSerializer.class.getName())
                .withClientIdConfig(kafkaUsername + "-producer")
                .withSecurityProtocol(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
                .withSharedProperties()
                .build(), resultPromise, msgCntPredicate, this.topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> sendMessagesTls() {
        return sendMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Send messages to external entrypoint of the cluster with SSL security protocol setting
     * @return future with sent message count
     */
    @Override
    public Future<Integer> sendMessagesTls(long timeoutMs) {

        String clientName = "sender-ssl" + this.clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(this.namespaceName, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        vertx.deployVerticle(new Producer(
            new KafkaClientProperties.KafkaClientPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
                .withKeySerializerConfig(StringSerializer.class.getName())
                .withValueSerializerConfig(StringSerializer.class.getName())
                .withClientIdConfig(kafkaUsername + "-producer")
                .withCaSecretName(caCertName)
                .withKafkaUsername(kafkaUsername)
                .withSecurityProtocol(securityProtocol)
                .withSaslMechanism("")
                .withSharedProperties()
                .build(),
            resultPromise, msgCntPredicate, this.topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> receiveMessagesPlain() {
        return receiveMessagesPlain(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Receive messages to external entrypoint of the cluster with PLAINTEXT security protocol setting
     * @return
     */
    @Override
    public Future<Integer> receiveMessagesPlain(long timeoutMs) {

        String clientName = "receiver-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        vertx.deployVerticle(new Consumer(
            new KafkaClientProperties.KafkaClientPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
                .withKeyDeSerializerConfig(StringDeserializer.class.getName())
                .withValueDeSerializerConfig(StringDeserializer.class.getName())
                .withClientIdConfig(kafkaUsername + "-consumer")
                .withAutoOffsetResetConfig("earliest")
                .withGroupIdConfig(consumerGroup)
                .withSecurityProtocol(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
                .withSharedProperties()
                .build(), resultPromise, msgCntPredicate, this.topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> receiveMessagesTls() {
        return receiveMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Receive messages to external entrypoint of the cluster with SSL security protocol setting
     * @return future with received message count
     */
    public Future<Integer> receiveMessagesTls(long timeoutMs) {

        String clientName = "receiver-ssl-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(this.namespaceName, this.clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        vertx.deployVerticle(new Consumer(
            new KafkaClientProperties.KafkaClientPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
                .withKeyDeSerializerConfig(StringDeserializer.class.getName())
                .withValueDeSerializerConfig(StringDeserializer.class.getName())
                .withClientIdConfig(kafkaUsername + "-consumer")
                .withAutoOffsetResetConfig("earliest")
                .withGroupIdConfig(consumerGroup)
                .withSecurityProtocol(securityProtocol)
                .withCaSecretName(caCertName)
                .withKafkaUsername(kafkaUsername)
                .withSaslMechanism("")
                .withSharedProperties()
                .build(), resultPromise, msgCntPredicate, this.topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    @Override
    public void close() {
        vertx.close();
    }

    @Override
    public String toString() {
        return "BasicKafkaClient{" +
                "topicName='" + topicName + '\'' +
                ", namespaceName='" + namespaceName + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", messageCount=" + messageCount +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", kafkaUsername='" + kafkaUsername + '\'' +
                ", securityProtocol='" + securityProtocol + '\'' +
                ", caCertName='" + caCertName + '\'' +
                '}';
    }
}
