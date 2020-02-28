/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.IKafkaClient;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.kafkaclients.internalClients.ClientType.CLI_KAFKA_VERIFIABLE_CONSUMER;
import static io.strimzi.systemtest.kafkaclients.internalClients.ClientType.CLI_KAFKA_VERIFIABLE_PRODUCER;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The InternalKafkaClient for sending and receiving messages using basic properties.
 * The client is using an internal listeners and communicate from the pod.
 */
public class InternalKafkaClient implements IKafkaClient<Integer> {

    private static final Logger LOGGER = LogManager.getLogger(InternalKafkaClient.class);

    private int sent;
    private int received;
    private Random rng;
    private String podName;

    public InternalKafkaClient() {
        this.sent = 0;
        this.received = 0;
        this.rng = new Random();
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param messageCount messages count
     * @param securityProtocol option for tls listener inside kafka cluster
     * @return count of send and acknowledged messages
     */
    private Integer sendMessages(String topicName, String namespace, String clusterName, String kafkaUsername,
                                 int messageCount, String securityProtocol, String podName, long timeoutMs) {
        String bootstrapServer = securityProtocol.equals("TLS") ?
                clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        ClientArgumentMap producerArguments = new ClientArgumentMap();
        producerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        producerArguments.put(ClientArgument.TOPIC, topicName);
        producerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        VerifiableClient producer = new VerifiableClient(CLI_KAFKA_VERIFIABLE_PRODUCER,
            podName,
            namespace);

        if (kafkaUsername != null) {
            producerArguments.put(ClientArgument.USER, kafkaUsername.replace("-", "_"));
        }

        producer.setArguments(producerArguments);
        LOGGER.info("Sending {} messages to {}#{}", messageCount, bootstrapServer, topicName);

        TestUtils.waitFor("Sending messages", Constants.PRODUCER_POLL_INTERVAL, Constants.GLOBAL_CLIENTS_TIMEOUT, () -> {
            LOGGER.info("Sending {} messages to {}", messageCount, podName);
            producer.run(Constants.PRODUCER_TIMEOUT);
            sent = getSentMessagesCount(producer.getMessages().toString(), messageCount);
            return sent == messageCount;
        });

        sent = getSentMessagesCount(producer.getMessages().toString(), messageCount);

        LOGGER.info("Producer produced {} messages", sent);

        return sent;
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param messageCount messages count
     * @param securityProtocol option for tls listener inside kafka cluster
     * @return count of send and acknowledged messages
     */
    public Integer sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                   int messageCount, String securityProtocol) {
        LOGGER.info("Sending messages to from: {}", this.podName);
        return sendMessages(topicName, namespace, clusterName, kafkaUsername, messageCount, securityProtocol,
                this.podName, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param messageCount messages count
     * @param securityProtocol option for tls listener inside kafka cluster
     * @return count of send and acknowledged messages
     */
    @Override
    public Integer sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                   int messageCount, String securityProtocol, long timeoutMs) throws RuntimeException {
        LOGGER.info("Sending messages to pod: {}", this.podName);
        return sendMessages(topicName, namespace, clusterName, kafkaUsername, messageCount, securityProtocol, this.podName,
                timeoutMs);
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param messageCount messages count
     * @return count of send and acknowledged messages
     */
    public Integer sendMessages(String topicName, String namespace, String clusterName, int messageCount) throws RuntimeException {
        return sendMessagesTls(topicName, namespace, clusterName, null, messageCount, "PLAIN",
                Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param messageCount messages count
     * @return count of send and acknowledged messages
     */
    @Override
    public Integer sendMessages(String topicName, String namespace, String clusterName, int messageCount, long timeoutMs) throws RuntimeException {
        return sendMessagesTls(topicName, namespace, clusterName, null, messageCount, "PLAIN",
                timeoutMs);
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param kafkaUsername kafka username
     * @param messageCount messages count
     * @return count of send and acknowledged messages
     */
    public Integer sendMessages(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount) throws InterruptedException {
        return sendMessagesTls(topicName, namespace, clusterName, kafkaUsername, messageCount, "PLAIN",
                Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param clusterName cluster name
     * @param messageCount message count
     * @param kafkaUsername user for tls if it's used for messages
     * @param securityProtocol option for tls listener inside kafka cluster
     * @return count of received messages
     */
    @Override
    public Integer receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                      int messageCount, String securityProtocol, String consumerGroup, long timeoutMs) {
        String bootstrapServer = securityProtocol.equals("TLS") ?
                clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        ClientArgumentMap consumerArguments = new ClientArgumentMap();
        consumerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        consumerArguments.put(ClientArgument.GROUP_ID, consumerGroup);

        String image = kubeClient().getPod(this.podName).getSpec().getContainers().get(0).getImage();
        String clientVersion = image.substring(image.length() - 5);

        if (allowParameter("2.3.0", clientVersion)) {
            consumerArguments.put(ClientArgument.GROUP_INSTANCE_ID, "instance" + rng.nextInt(Integer.MAX_VALUE));
        }
        consumerArguments.put(ClientArgument.VERBOSE, "");
        consumerArguments.put(ClientArgument.TOPIC, topicName);
        consumerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));


        LOGGER.info("Receiving from this pod....{}", this.podName);
        VerifiableClient consumer = new VerifiableClient(CLI_KAFKA_VERIFIABLE_CONSUMER,
            this.podName,
            namespace);

        if (kafkaUsername != null) {
            consumerArguments.put(ClientArgument.USER, kafkaUsername.replace("-", "_"));
        }

        consumer.setArguments(consumerArguments);

        LOGGER.info("Wait for receive {} messages from {}#{}", messageCount, bootstrapServer, topicName);

        boolean hasPassed = consumer.run(timeoutMs);
        LOGGER.info("Consumer finished correctly: {}", hasPassed);

        received = getReceivedMessagesCount(consumer.getMessages().toString());
        LOGGER.info("Consumer consumed {} messages", received);

        return received;
    }


    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param topicName topic name
     * @return count of received messages
     */
    public Integer receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUserName,
                                      int messageCount, String securityProtocol, String consumerGroup) {
        return receiveMessagesTls(topicName, namespace, clusterName, kafkaUserName, messageCount, securityProtocol,
                consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param topicName topic name
     * @return count of received messages
     */
    public Integer receiveMessagesTls(String topicName, String namespace, String clusterName, int messageCount,
                                      String consumerGroup) {
        return receiveMessagesTls(topicName, namespace, clusterName, null, messageCount, "TLS",
                consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param topicName topic name
     * @return count of received messages
     */
    public Integer receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                   String consumerGroup) {
        return receiveMessagesTls(topicName, namespace, clusterName, null, messageCount, "PLAIN",
                consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount message count
     * @param clusterName cluster name
     * @param topicName topic name
     * @return count of received messages
     */
    @Override
    public Integer receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                   String consumerGroup, long timeoutMs) {
        return receiveMessagesTls(topicName, namespace, clusterName, null, messageCount, "PLAIN",
                consumerGroup, timeoutMs);
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param topicName topic name
     * @param namespace namespace
     * @param messageCount message count
     * @param clusterName cluster name
     * @param kafkaUsername kafka username
     * @param consumerGroup consumergroup name
     * @return count of received messages
     */
    public Integer receiveMessages(String topicName, String namespace, String clusterName, String kafkaUsername, int messageCount, String consumerGroup) {
        return receiveMessagesTls(topicName, namespace, clusterName, kafkaUsername, messageCount, "PLAIN", consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    public void checkProducedAndConsumedMessages(int producedMesssages, int consumedMessages) {
        assertSentAndReceivedMessages(producedMesssages, consumedMessages);
    }

    /**
     * Assert count of sent and received messages
     * @param sent count of sent messages
     * @param received count of received messages
     */
    public void assertSentAndReceivedMessages(int sent, int received) {
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

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }
}
