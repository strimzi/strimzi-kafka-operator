/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeClientsResource;
import io.strimzi.systemtest.utils.ClientUtils;
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
    public static String bridgeServiceName = KafkaBridgeResources.serviceName(CLUSTER_NAME);
    public static String bridgeUrl = "";

    public static String producerName = "bridge-producer";
    public static String consumerName = "bridge-consumer";

    protected WebClient client;
    protected static KafkaBridgeClientsResource kafkaBridgeClientJob;

    void deployClusterOperator(String namespace) throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(namespace);
    }

    @BeforeAll
    void createClassResources() throws Exception {
        deployClusterOperator(NAMESPACE);

        kafkaBridgeClientJob = new KafkaBridgeClientsResource(producerName, consumerName, KafkaBridgeResources.serviceName(CLUSTER_NAME),
                TOPIC_NAME, MESSAGE_COUNT, "", ClientUtils.generateRandomConsumerGroup(), bridgePort, 1000, 1000);
    }
}
