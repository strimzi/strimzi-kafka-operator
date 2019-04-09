/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.apiclients.MsgCliApiClient;
import io.strimzi.systemtest.kafkaclients.ClientArgument;
import io.strimzi.systemtest.kafkaclients.ClientArgumentMap;
import io.strimzi.systemtest.kafkaclients.VerifiableClient;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.kafkaclients.ClientType.CLI_KAFKA_VERIFIABLE_CONSUMER;
import static io.strimzi.systemtest.kafkaclients.ClientType.CLI_KAFKA_VERIFIABLE_PRODUCER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Base for test classes where sending and receiving messages is used.
 */
public class MessagingBaseST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(MessagingBaseST.class);

    private MsgCliApiClient cliApiClient;
    private JsonObject response;
    private int sent = 0;
    private int received = 0;

    public JsonObject getResponse() {
        return response;
    }

    public void setResponse(JsonObject response) {
        this.response = response;
    }

    @BeforeAll
    public void setUpClientBase() throws MalformedURLException {
        String clientUrl = ENVIRONMENT.getKubernetesDomain().equals(Environment.KUBERNETES_DOMAIN_DEFAULT) ?  new URL(ENVIRONMENT.getKubernetesApiUrl()).getHost() + Environment.KUBERNETES_DOMAIN_DEFAULT : ENVIRONMENT.getKubernetesDomain();
        cliApiClient = new MsgCliApiClient(new URL("http://" + KAFKA_CLIENTS + "." + clientUrl + ":80"));
    }

    /**
     * Simple availability check for kafka cluster
     * @param clusterName cluster name
     */
    void availabilityTest(String clusterName) throws Exception {
        availabilityTest(100, 20000, clusterName, false, "my-topic", null);
    }

    /**
     * Simple availability check for kafka cluster
     * @param messageCount message count
     * @param timeout timeout
     * @param clusterName cluster name
     */
    void availabilityTest(int messageCount, int timeout, String clusterName) throws Exception {
        availabilityTest(messageCount, timeout, clusterName, false, "my-topic", null);
    }

    /**
     * Simple availability check for kafka cluster
     * @param messageCount message count
     * @param timeout timeout for producer and consumer to be finished
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     */
    void availabilityTest(int messageCount, int timeout, String clusterName, boolean tlsListener, String topicName, KafkaUser user) throws Exception {
        sendMessages(messageCount, timeout, clusterName, tlsListener, topicName, user);
        receiveMessages(messageCount, timeout, clusterName, tlsListener, topicName, user);
        assertSentAndReceivedMessages(sent, received);
    }

    /**
     * Method for send messages to specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount messages count
     * @param timeout timeout for producer to be finished
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     * @return count of send and acknowledged messages
     */
    int sendMessages(int messageCount, int timeout, String clusterName, boolean tlsListener, String topicName, KafkaUser user) throws Exception {
        String bootstrapServer = tlsListener ? clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        ClientArgumentMap producerArguments = new ClientArgumentMap();
        producerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        producerArguments.put(ClientArgument.TOPIC, topicName);
        producerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        VerifiableClient producer = new VerifiableClient(CLI_KAFKA_VERIFIABLE_PRODUCER);

        if (user != null) {
            producerArguments.put(ClientArgument.USER, user.getMetadata().getName().replace("-", "_"));
        }

        producer.setArguments(producerArguments);

        LOGGER.info("Sending {} messages to {}#{}", messageCount, bootstrapServer, topicName);
        response = cliApiClient.sendAndGetStatus(producer);

        waitTillProcessFinish(getClientUUID(response), "producer", timeout);

        assertThat(String.format("Return code of sender is not 0: %s", response),
                response.getInteger("ecode"), is(0));

        sent = getSentMessagesCount(response, messageCount);

        assertThat(String.format("Sent (%s) and expected (%s) message count is not equal", sent, messageCount),
                sent == messageCount);

        LOGGER.info("Sent {} messages", sent);
        return sent;
    }

    /**
     * Method for receive messages from specific kafka cluster. It uses test-client API for communication with deployed clients inside kubernetes cluster
     * @param messageCount message count
     * @param timeout timeout for consumer to be finished
     * @param clusterName cluster name
     * @param tlsListener option for tls listener inside kafka cluster
     * @param topicName topic name
     * @param user user for tls if it's used for messages
     * @return count of received messages
     */
    int receiveMessages(int messageCount, int timeout, String clusterName, boolean tlsListener, String topicName, KafkaUser user) throws Exception {
        String bootstrapServer = tlsListener ? clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        ClientArgumentMap consumerArguments = new ClientArgumentMap();
        consumerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        consumerArguments.put(ClientArgument.GROUP_ID, "my-group" + rng.nextInt(Integer.MAX_VALUE));
        consumerArguments.put(ClientArgument.VERBOSE, "");
        consumerArguments.put(ClientArgument.TOPIC, topicName);
        consumerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        VerifiableClient consumer = new VerifiableClient(CLI_KAFKA_VERIFIABLE_CONSUMER);

        if (user != null) {
            consumerArguments.put(ClientArgument.USER, user.getMetadata().getName().replace("-", "_"));
        }

        consumer.setArguments(consumerArguments);

        LOGGER.info("Wait for receive {} messages from {}#{}", messageCount, bootstrapServer, topicName);
        response = cliApiClient.sendAndGetStatus(consumer);

        waitTillProcessFinish(getClientUUID(response), "consumer", timeout);

        assertThat(String.format("Return code of receiver is not 0: %s", response),
                response.getInteger("ecode"), is(0));

        received = getReceivedMessagesCount(response);

        assertThat(String.format("Received (%s) and expected (%s) message count is not equal", sent, messageCount),
                sent == messageCount);

        LOGGER.info("Received {} messages", received);
        return received;
    }

    private String getClientUUID(JsonObject response) {
        return response.getString("UUID");
    }

    /**
     * Checks if process containing producer/consumer inside client pod finished or not
     * @param processUuid process uuid
     * @param description description for wait method
     * @param timeout timeout
     */
    private void waitTillProcessFinish(String processUuid, String description, int timeout) {
        TestUtils.waitFor("Wait till " + description + " finished", 2000, timeout, () -> {
            JsonObject out;
            try {
                out = cliApiClient.getClientInfo(processUuid);
                setResponse(out);
                return !out.getBoolean("isRunning");
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });
    }

    /**
     * Get sent messages fro object response
     * @param response response
     * @param messageCount expected message count
     * @return count of acknowledged messages
     */
    private int getSentMessagesCount(JsonObject response, int messageCount) {
        int sentMessages;
        String sentPattern = String.format("sent\":(%s)", messageCount);
        String ackPattern = String.format("acked\":(%s)", messageCount);
        Pattern r = Pattern.compile(sentPattern);
        Matcher m = r.matcher(response.getString("stdOut"));
        sentMessages = m.find() ? Integer.parseInt(m.group(1)) : -1;

        r = Pattern.compile(ackPattern);
        m = r.matcher(response.getString("stdOut"));

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
    private int getReceivedMessagesCount(JsonObject response) {
        int receivedMessages = 0;
        String pattern = String.format("records_consumed\",\"count\":([0-9]*)");
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(response.getString("stdOut"));
        while (m.find()) {
            receivedMessages += Integer.parseInt(m.group(1));
        }
        return receivedMessages;
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
}
