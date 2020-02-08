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
 * The OauthExternalKafkaClient for sending and receiving messages using access token provided by authorization server.
 * The client is using an external listeners.
 */
public class OauthExternalKafkaClient extends AbstractKafkaClient implements IKafkaClientOperations<Future<Integer>> {

    private static final Logger LOGGER = LogManager.getLogger(OauthExternalKafkaClient.class);
    private Vertx vertx = Vertx.vertx();

    private String clientId;
    private String clientSecretName;
    private String oauthTokenEndpointUri;
    private String introspectionEndpointUri;

    public static class Builder extends AbstractKafkaClient.Builder<Builder> {

        private String clientId;
        private String clientSecretName;
        private String oauthTokenEndpointUri;
        private String introspectionEndpointUri;

        public Builder withOauthClientId(String oauthClientId) {

            this.clientId = oauthClientId;
            return self();
        }

        public Builder withClientSecretName(String clientSecretName) {

            this.clientSecretName = clientSecretName;
            return self();
        }

        public Builder withOauthTokenEndpointUri(String oauthTokenEndpointUri) {

            this.oauthTokenEndpointUri = oauthTokenEndpointUri;
            return self();
        }

        public Builder withIntrospectionEndpointUri(String introspectionEndpointUri) {

            this.introspectionEndpointUri = introspectionEndpointUri;
            return self();
        }

        @Override
        public OauthExternalKafkaClient build() {

            return new OauthExternalKafkaClient(this);
        }

        @Override
        protected Builder self() {

            return this;
        }
    }

    private OauthExternalKafkaClient(Builder builder) {

        super(builder);
        clientId = builder.clientId;
        clientSecretName = builder.clientSecretName;
        oauthTokenEndpointUri = builder.oauthTokenEndpointUri;
        introspectionEndpointUri = builder.introspectionEndpointUri;
    }

    public Future<Integer> sendMessagesPlain() {
        return sendMessagesPlain(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public Future<Integer> sendMessagesPlain(long timeoutMs) {
        String clientName = "sender-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        KafkaClientProperties kafkaClientProperties = new KafkaClientProperties.KafkaClientPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withSecurityProtocol("SASL_" + CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
            .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
            .withKeySerializerConfig(StringSerializer.class.getName())
            .withValueSerializerConfig(StringSerializer.class.getName())
            .withClientIdConfig(kafkaUsername + "-producer")
            .withSaslMechanism("OAUTHBEARER")
            .withSaslLoginCallbackHandlerClass()
            .withSharedProperties()
            .withSaslJassConfig(this.clientId, this.clientSecretName, this.oauthTokenEndpointUri)
            .build();

        vertx.deployVerticle(new Producer(kafkaClientProperties, resultPromise, msgCntPredicate, topicName, clientName));

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

    @Override
    public Future<Integer> sendMessagesTls(long timeoutMs) {
        String clientName = "sender-ssl" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(namespaceName, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);


        KafkaClientProperties kafkaClientProperties = new KafkaClientProperties.KafkaClientPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
            .withKeySerializerConfig(StringSerializer.class.getName())
            .withValueSerializerConfig(StringSerializer.class.getName())
            .withCaSecretName(caCertName)
            .withKafkaUsername(kafkaUsername)
            .withSecurityProtocol("SASL_SSL")
            .withClientIdConfig(kafkaUsername + "-producer")
            .withSaslMechanism("OAUTHBEARER")
            .withSaslLoginCallbackHandlerClass()
            .withSharedProperties()
            .withSaslJassConfigAndTls(clientId, clientSecretName, oauthTokenEndpointUri)
            .build();

        vertx.deployVerticle(new Producer(kafkaClientProperties, resultPromise, msgCntPredicate, topicName, clientName));

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

    @Override
    public Future<Integer> receiveMessagesPlain(long timeoutMs) {
        String clientName = "receiver-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        KafkaClientProperties kafkaClientProperties = new KafkaClientProperties.KafkaClientPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withGroupIdConfig(consumerGroup)
            .withSecurityProtocol("SASL_" + CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
            .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
            .withKeyDeSerializerConfig(StringDeserializer.class.getName())
            .withValueDeSerializerConfig(StringDeserializer.class.getName())
            .withClientIdConfig(kafkaUsername + "-consumer")
            .withAutoOffsetResetConfig("earliest")
            .withSaslMechanism("OAUTHBEARER")
            .withSaslLoginCallbackHandlerClass()
            .withSharedProperties()
            .withSaslJassConfig(this.clientId, this.clientSecretName, this.oauthTokenEndpointUri)
            .build();

        vertx.deployVerticle(new Consumer(kafkaClientProperties, resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> receiveMessagesTls() {
        return sendMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public Future<Integer> receiveMessagesTls(long timeoutMs) {

        String clientName = "receiver-ssl-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(namespaceName, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        KafkaClientProperties kafkaClientProperties = new KafkaClientProperties.KafkaClientPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withCaSecretName(caCertName)
            .withBootstrapServerConfig(getExternalBootstrapConnect(namespaceName, clusterName))
            .withKeyDeSerializerConfig(StringDeserializer.class.getName())
            .withValueDeSerializerConfig(StringDeserializer.class.getName())
            .withKafkaUsername(kafkaUsername)
            .withSecurityProtocol("SASL_SSL")
            .withGroupIdConfig(consumerGroup)
            .withAutoOffsetResetConfig("earliest")
            .withClientIdConfig(kafkaUsername + "-consumer")
            .withSaslMechanism("OAUTHBEARER")
            .withSaslLoginCallbackHandlerClass()
            .withSharedProperties()
            .withSaslJassConfigAndTls(this.clientId, this.clientSecretName, this.oauthTokenEndpointUri)
            .build();

        vertx.deployVerticle(new Consumer(kafkaClientProperties, resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    @Override
    public String toString() {

        return "OauthKafkaClient{" +
                "oauthTokenEndpointUri='" + oauthTokenEndpointUri + '\'' +
                ", introspectionEndpointUri='" + introspectionEndpointUri + '\'' +
                ", topicName='" + topicName + '\'' +
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
