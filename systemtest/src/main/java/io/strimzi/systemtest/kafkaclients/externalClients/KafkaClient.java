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
import java.security.KeyStoreException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntPredicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

    public void sendMessagesExternal(String clusterName, String namespace, String topicName, int messageCount) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessages(topicName, namespace, clusterName, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void sendMessagesExternalTls(String clusterName, String namespace, String topicName, int messageCount,
                                        String userName) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void sendMessagesExternalScramSha(String clusterName, String namespace, String topicName, int messageCount,
                                             String userName) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SASL_SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Receive messages to external entrypoint of the cluster with PLAINTEXT security protocol setting
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param messageCount message count
     * @param consumerGroup consumer group name
     * @return future with received message count
     */
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                           String consumerGroup) throws IOException {
        return receiveMessages(topicName, namespace, clusterName, messageCount, consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
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
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
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
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String userName,
                                              int messageCount, String securityProtocol) throws IOException {
        return receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, securityProtocol,
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE), 120);
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
                caCertName, kafkaUsername, securityProtocol, consumerGroup),
                resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public void receiveMessagesExternal(String clusterName, String namespace, String topicName, int messageCount, String consumerGroup) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future consumer = testClient.receiveMessages(topicName, namespace, clusterName, messageCount, consumerGroup);

            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternal(String clusterName, String namespace, String topicName, int messageCount) throws Exception {
        receiveMessagesExternal(clusterName, namespace, topicName, messageCount, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    public void receiveMessagesExternalTls(String clusterName, String namespace, String topicName, int messageCount, String userName, String consumerGroup) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SSL", consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);

            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternalTls(String clusterName, String namespace, String topicName, int messageCount, String userName) throws Exception {
        receiveMessagesExternalTls(clusterName, namespace, topicName, messageCount, userName, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    public void receiveMessagesExternalScramSha(String clusterName, String namespace, String topicName, int messageCount, String userName, String consumerGroup) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SASL_SSL", consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);

            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternalScramSha(String clusterName, String namespace, String topicName, int messageCount, String userName) throws Exception {
        receiveMessagesExternalScramSha(clusterName, namespace, topicName, messageCount, userName, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    /**
     * Receive messages from external entrypoint until stop notification is received by consumer. SSL used as a security protocol setting.
     * @param topicName topic name
     * @param namespace kafka namespace
     * @param clusterName kafka cluster name
     * @param userName user name for authorization
     * @param clientName client name
     * @return future
     */
    public CompletableFuture<Integer> receiveMessagesUntilNotification(String topicName, String namespace, String clusterName, String userName, String clientName, String securityProtocol) {
        return new CompletableFuture<>();
    }

    /**
     * Send notification to vert.x event bus
     * @param clientName client name as a vert.x even bus address
     * @param notification notification
     */
    public void sendNotificationToClient(String clientName, String notification) {
        vertx = Vertx.vertx();
        LOGGER.debug("Sending {} to {}", notification, clientName);
        vertx.eventBus().publish(clientName, notification);
        vertx.close();
    }

    /**
     * Wait for cluster availability, check availability of external routes with TLS
     * @param userName user name
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @param messageCount message count which will be send and receive by consumer and producer
     * @throws Exception exception
     */
    public void sendAndRecvMessagesTls(String userName, String namespace, String clusterName, String topicName, int messageCount) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SSL");
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Wait for cluster availability, check availability of external routes with SCRAM-SHA
     * @param userName user name
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @param messageCount message count which will be send and receive by consumer and producer
     * @throws Exception exception
     */
    public void sendAndRecvMessagesScramSha(String userName, String namespace, String clusterName, String topicName, int messageCount) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SASL_SSL");
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SASL_SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Wait for cluster availability, check availability of external routes with TLS
     * @param userName user name
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @throws Exception exception
     */
    public void sendAndRecvMessagesTls(String userName, String namespace, String clusterName, String topicName) throws InterruptedException, ExecutionException, TimeoutException, KeyStoreException, IOException {
        sendAndRecvMessagesTls(userName, namespace, clusterName, topicName, 50);
    }

    /**
     * Wait for cluster availability, check availability of external routes with TLS
     * @param userName user name
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @throws Exception exception
     */
    public void sendAndRecvMessagesTls(String userName, String namespace, String clusterName) throws InterruptedException, ExecutionException, TimeoutException, KeyStoreException, IOException {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        sendAndRecvMessagesTls(userName, namespace, clusterName, topicName);
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @throws Exception
     */
    public void sendAndRecvMessages(String namespace) throws Exception {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        sendAndRecvMessages(namespace, topicName);
    }


    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param topicName topic name
     * @throws Exception
     */
    public void sendAndRecvMessages(String namespace, String topicName) throws Exception {
        sendAndRecvMessages(namespace, "my-cluster", topicName);
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @throws Exception
     */
    public void sendAndRecvMessages(String namespace, String clusterName, String topicName) throws Exception {
        sendAndRecvMessages(namespace, clusterName, topicName, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @param consumerGroup consumer group
     * @throws Exception
     */
    public void sendAndRecvMessages(String namespace, String clusterName, String topicName, String consumerGroup) throws Exception {
        int messageCount = 50;

        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessages(topicName, namespace, clusterName, messageCount);
            Future consumer = testClient.receiveMessages(topicName, namespace, clusterName, messageCount, consumerGroup);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @param messageCount message count which will be send and receive by consumer and producer
     * @throws Exception exception
     */
    public void sendAndRecvMessages(String namespace, String clusterName, String topicName, int messageCount) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessages(topicName, namespace, clusterName, messageCount);
            Future consumer = testClient.receiveMessages(topicName, namespace, clusterName, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public String getCaCertName() {
        return caCertName;
    }

    public void setCaCertName(String caCertName) {
        this.caCertName = caCertName;
    }
}
