/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.apiclients.MsgCliApiClient;
import io.strimzi.systemtest.kafkaclients.AbstractClient;
import io.strimzi.systemtest.kafkaclients.ClientArgument;
import io.strimzi.systemtest.kafkaclients.ClientArgumentMap;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;

import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MessagingBaseST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);

    private MsgCliApiClient cliApiClient;

    private ClientArgumentMap producerArguments = new ClientArgumentMap();
    private ClientArgumentMap consumerArguments = new ClientArgumentMap();

    private JsonObject consumerResponse;

    public JsonObject getConsumerResponse() {
        return consumerResponse;
    }

    public void setConsumerResponse(JsonObject consumerResponse) {
        this.consumerResponse = consumerResponse;
    }

    @BeforeAll
    public void setUpClientBase() throws Exception {
        cliApiClient = new MsgCliApiClient(new URL("http://kafka-clients.127.0.0.1.nip.io:80"));
    }


    void availabilityTest(AbstractClient producer, AbstractClient consumer, String clusterName) throws InterruptedException, ExecutionException, TimeoutException {
        availabilityTest(producer, consumer, 100, 20000, clusterName, false, "my-topic");
    }

    void availabilityTest(AbstractClient producer, AbstractClient consumer, int messageCount, int timeout, String clusterName) throws InterruptedException, ExecutionException, TimeoutException {
        availabilityTest(producer, consumer, messageCount, timeout, clusterName, false, "my-topic");
    }

    void availabilityTest(AbstractClient producer, AbstractClient consumer, int messageCount, int timeout, String clusterName, boolean tlsListener, String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        String bootstrapServer = tlsListener ? clusterName + "-kafka-bootstrap:9093" : clusterName + "-kafka-bootstrap:9092";
        producerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        producerArguments.put(ClientArgument.TOPIC, topicName);
        producerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        consumerArguments.put(ClientArgument.BROKER_LIST, bootstrapServer);
        consumerArguments.put(ClientArgument.GROUP_ID, "my-group");
        consumerArguments.put(ClientArgument.VERBOSE, "");
        consumerArguments.put(ClientArgument.TOPIC, topicName);
        consumerArguments.put(ClientArgument.MAX_MESSAGES, Integer.toString(messageCount));

        producer.setArguments(producerArguments);
        consumer.setArguments(consumerArguments);

        JsonObject response = cliApiClient.sendAndGetStatus(producer);

        waitTillProcessFinish(getClientUUID(response), timeout);

        assertThat(String.format("Return code of sender is not 0: {}", consumerResponse),
                consumerResponse.getInteger("ecode"), is(0));

        response = cliApiClient.sendAndGetStatus(consumer);

        waitTillProcessFinish(getClientUUID(response), timeout);

        assertThat(String.format("Return code of receiver is not 0: {}", consumerResponse),
                consumerResponse.getInteger("ecode"), is(0));
    }

    private String getClientUUID(JsonObject response) {
        return response.getString("UUID");
    }

    private void waitTillProcessFinish(String processUuid, int timeout) {
        TestUtils.waitFor("Wait till producer finished", 2000, timeout, () -> {
            JsonObject out = null;
            try {
                out = cliApiClient.getClientInfo(processUuid);
                setConsumerResponse(out);
                return !out.getBoolean("isRunning");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                return false;
            }
        });
    }
}
