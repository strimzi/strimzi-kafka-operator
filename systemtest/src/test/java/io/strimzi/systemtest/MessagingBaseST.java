/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.kafkaclients.api.ClientArgument;
import io.strimzi.systemtest.kafkaclients.api.ClientArgumentMap;
import io.strimzi.systemtest.kafkaclients.api.VerifiableClient;
import io.strimzi.systemtest.kafkaclients.lib.KafkaClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.kafkaclients.api.ClientType.CLI_KAFKA_VERIFIABLE_CONSUMER;
import static io.strimzi.systemtest.kafkaclients.api.ClientType.CLI_KAFKA_VERIFIABLE_PRODUCER;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Base for test classes where sending and receiving messages is used.
 */
public class MessagingBaseST extends BaseST {
    private static final Logger LOGGER = LogManager.getLogger(MessagingBaseST.class);

    private int sent = 0;
    private int received = 0;

    /**
     * Simple availability check for kafka cluster
     * @param clusterName cluster name
     */
    public void availabilityTest(String clusterName) throws Exception {
        availabilityTest(100, clusterName, false, "my-topic", null);
    }

    /**
     * Simple availability check for kafka cluster
     * @param messageCount message count
     * @param clusterName cluster name
     */
    protected void availabilityTest(int messageCount, String clusterName) throws Exception {
        availabilityTest(messageCount, clusterName, false, "my-topic", null);
    }

    /**
     * Simple availability check for kafka cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param topicName topic name
     */
    protected void availabilityTest(int messageCount, String clusterName, String topicName) throws Exception {
        availabilityTest(messageCount, clusterName, false, topicName, null);
    }

    /**
     * Simple availability check for kafka cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     */
    void availabilityTest(int messageCount, String clusterName, boolean tlsListener, String topicName, KafkaUser user) throws Exception {
        final String  defaultKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();
        sent = sendMessages(messageCount, clusterName, tlsListener, topicName, user, defaultKafkaClientsPodName);
        received = receiveMessages(messageCount, clusterName, tlsListener, topicName, user, defaultKafkaClientsPodName);
        assertSentAndReceivedMessages(sent, received);
    }

    /**
     * Simple availability check for kafka cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     * @param podName name of the pod
     */
    void availabilityTest(int messageCount, String clusterName, boolean tlsListener, String topicName, KafkaUser user, String podName) throws Exception {
        sent = sendMessages(messageCount, clusterName, tlsListener, topicName, user, podName);
        received = receiveMessages(messageCount, clusterName, tlsListener, topicName, user, podName);
        assertSentAndReceivedMessages(sent, received);
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount messages count
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     * @return count of send and acknowledged messages
     */
    int sendMessages(int messageCount, String clusterName, boolean tlsListener, String topicName, KafkaUser user, String podName) throws Exception {
        String bootstrapServer = tlsListener ? clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        ClientArgumentMap producerArguments = new ClientArgumentMap();
        producerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        producerArguments.put(ClientArgument.TOPIC, topicName);
        producerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        VerifiableClient producer = new VerifiableClient(CLI_KAFKA_VERIFIABLE_PRODUCER,
                podName,
                kubeClient().getNamespace());

        if (user != null) {
            producerArguments.put(ClientArgument.USER, user.getMetadata().getName().replace("-", "_"));
        }

        producer.setArguments(producerArguments);
        LOGGER.info("Sending {} messages to {}#{}", messageCount, bootstrapServer, topicName);

        boolean hasPassed = producer.run();
        LOGGER.info("Producer finished correctly: {}", hasPassed);

        sent = getSentMessagesCount(producer.getMessages().toString(), messageCount);
        LOGGER.info("Producer produced {} messages", sent);

        return sent;
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     * @return count of received messages
     */
    int receiveMessages(int messageCount, String clusterName, boolean tlsListener, String topicName, KafkaUser user, String podName) {
        String bootstrapServer = tlsListener ? clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        ClientArgumentMap consumerArguments = new ClientArgumentMap();
        consumerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        consumerArguments.put(ClientArgument.GROUP_ID, "my-group" + rng.nextInt(Integer.MAX_VALUE));

        String image = kubeClient().getPod(podName).getSpec().getContainers().get(0).getImage();
        String clientVersion = image.substring(image.length() - 5);

        if (allowParameter("2.3.0", clientVersion)) {
            consumerArguments.put(ClientArgument.GROUP_INSTANCE_ID, "instance" + rng.nextInt(Integer.MAX_VALUE));
        }
        consumerArguments.put(ClientArgument.VERBOSE, "");
        consumerArguments.put(ClientArgument.TOPIC, topicName);
        consumerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        VerifiableClient consumer = new VerifiableClient(CLI_KAFKA_VERIFIABLE_CONSUMER,
                podName,
                kubeClient().getNamespace());

        if (user != null) {
            consumerArguments.put(ClientArgument.USER, user.getMetadata().getName().replace("-", "_"));
        }

        consumer.setArguments(consumerArguments);

        LOGGER.info("Wait for receive {} messages from {}#{}", messageCount, bootstrapServer, topicName);

        boolean hasPassed = consumer.run();
        LOGGER.info("Consumer finished correctly: {}", hasPassed);

        received = getReceivedMessagesCount(consumer.getMessages().toString());
        LOGGER.info("Consumer consumed {} messages", received);

        return received;
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
    public void waitForClusterAvailabilityTls(String userName, String namespace, String clusterName, String topicName, int messageCount) throws InterruptedException, ExecutionException, TimeoutException {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SSL");
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
    public void waitForClusterAvailabilityScramSha(String userName, String namespace, String clusterName, String topicName, int messageCount) throws InterruptedException, ExecutionException, TimeoutException {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SASL_SSL");
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, clusterName, userName, messageCount, "SASL_SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
    public void waitForClusterAvailabilityTls(String userName, String namespace, String clusterName, String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        waitForClusterAvailabilityTls(userName, namespace, clusterName, topicName, 50);
    }

    /**
     * Wait for cluster availability, check availability of external routes with TLS
     * @param userName user name
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @throws Exception exception
     */
    public void waitForClusterAvailabilityTls(String userName, String namespace, String clusterName) throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        waitForClusterAvailabilityTls(userName, namespace, clusterName, topicName);
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @throws Exception
     */
    public void waitForClusterAvailability(String namespace) throws Exception {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        waitForClusterAvailability(namespace, topicName);
    }


    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param topicName topic name
     * @throws Exception
     */
    public void waitForClusterAvailability(String namespace, String topicName) throws Exception {
        waitForClusterAvailability(namespace, CLUSTER_NAME, topicName);
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @throws Exception
     */
    public void waitForClusterAvailability(String namespace, String clusterName, String topicName) throws Exception {
        waitForClusterAvailability(namespace, clusterName, topicName, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @param clusterName cluster name
     * @param topicName topic name
     * @param consumerGroup consumer group
     * @throws Exception
     */
    public void waitForClusterAvailability(String namespace, String clusterName, String topicName, String consumerGroup) throws Exception {
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
    public void waitForClusterAvailability(String namespace, String clusterName, String topicName, int messageCount) throws Exception {
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

    public void sendMessagesExternal(String namespace, String topicName, int messageCount) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessages(topicName, namespace, CLUSTER_NAME, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void sendMessagesExternalTls(String namespace, String topicName, int messageCount, String userName) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount, "SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void sendMessagesExternalScramSha(String namespace, String topicName, int messageCount, String userName) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future producer = testClient.sendMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount, "SASL_SSL");

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternal(String namespace, String topicName, int messageCount, String consumerGroup) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future consumer = testClient.receiveMessages(topicName, namespace, CLUSTER_NAME, messageCount, consumerGroup);

            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternal(String namespace, String topicName, int messageCount) throws Exception {
        receiveMessagesExternal(namespace, topicName, messageCount, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    public void receiveMessagesExternalTls(String namespace, String topicName, int messageCount, String userName, String consumerGroup) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount, "SSL", consumerGroup);

            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternalTls(String namespace, String topicName, int messageCount, String userName) throws Exception {
        receiveMessagesExternalTls(namespace, topicName, messageCount, userName, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    public void receiveMessagesExternalScramSha(String namespace, String topicName, int messageCount, String userName, String consumerGroup) throws Exception {
        try (KafkaClient testClient = new KafkaClient()) {
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount, "SASL_SSL", consumerGroup);

            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void receiveMessagesExternalScramSha(String namespace, String topicName, int messageCount, String userName) throws Exception {
        receiveMessagesExternalScramSha(namespace, topicName, messageCount, userName, "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    /**
     * Assert count of sent and received messages
     * @param sent count of sent messages
     * @param received count of received messages
     */
    void assertSentAndReceivedMessages(int sent, int received) {
        assertThat(String.format("Sent (%s) and receive (%s) message count is not equal", sent, received),
                sent == received);
    }

    private boolean allowParameter(String minimalVersion, String clientVersion) {
        Pattern pattern = Pattern.compile("(?<major>[0-9]).(?<minor>[0-9]).(?<micro>[0-9])");
        Matcher current = pattern.matcher(clientVersion);
        Matcher minimal = pattern.matcher(minimalVersion);
        if (current.find() && minimal.find()) {
            return Integer.parseInt(current.group("major")) >= Integer.parseInt(minimal.group("major"))
                    && Integer.parseInt(current.group("minor")) >= Integer.parseInt(minimal.group("minor"))
                    && Integer.parseInt(current.group("micro")) >= Integer.parseInt(minimal.group("micro"));
        }
        return false;
    }

    /**
     * Get sent messages fro object response
     * @param response response
     * @param messageCount expected message count
     * @return count of acknowledged messages
     */
    private int getSentMessagesCount(String response, int messageCount) {
        int sentMessages;
        Pattern r = Pattern.compile("sent\":(" + messageCount + ")");
        Matcher m = r.matcher(response);
        sentMessages = m.find() ? Integer.parseInt(m.group(1)) : -1;

        r = Pattern.compile("acked\":(" + messageCount + ")");
        m = r.matcher(response);

        if (m.find()) {
            return sentMessages == Integer.parseInt(m.group(1)) ? sentMessages : -1;
        } else {
            return -1;
        }

    }

    /**
     * Get recieved message count from object response
     * @param response response
     * @return count of received messages
     */
    private int getReceivedMessagesCount(String response) {
        int receivedMessages = 0;
        Pattern r = Pattern.compile("records_consumed\",\"count\":([0-9]*)");
        Matcher m = r.matcher(response);

        while (m.find()) {
            receivedMessages += Integer.parseInt(m.group(1));
        }

        return receivedMessages;
    }
}
