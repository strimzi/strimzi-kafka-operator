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
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeAbstractST extends AbstractST {
    protected static final String NAMESPACE = "bridge-cluster-test";

    public static int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;
    public static String kafkaClientsPodName = "";
    public static String bridgeServiceName = KafkaBridgeResources.serviceName(clusterName);
    public static String bridgeUrl = "";

    public static String producerName = "bridge-producer";
    public static String consumerName = "bridge-consumer";

    protected WebClient client;
    protected static KafkaBridgeExampleClients kafkaBridgeClientJob;

    @BeforeAll
    void createBridgeClient() {
        kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(clusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();
    }
}
