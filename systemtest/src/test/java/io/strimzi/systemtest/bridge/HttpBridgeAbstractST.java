/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeAbstractST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeKafkaExternalListenersST.class);
    protected static final String NAMESPACE = "bridge-cluster-test";

    public static int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;
    public static String kafkaClientsPodName = "";
    protected static final String CLUSTER_NAME = "http-bridge-cluster-name";
    public static String bridgeUrl = "";

    public static String producerName = "bridge-producer";
    public static String consumerName = "bridge-consumer";

    protected WebClient client;
    protected static KafkaBridgeExampleClients kafkaBridgeClientJob;

    @BeforeAll
    void createBridgeClient() {
        LOGGER.debug("===============================================================");
        LOGGER.debug("{} - [BEFORE ALL] has been called", this.getClass().getName());

        kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(CLUSTER_NAME))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();
    }
}
