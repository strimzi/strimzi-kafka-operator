/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.vertx.ext.web.client.WebClient;
import io.strimzi.systemtest.resources.ResourceManager;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeAbstractST extends AbstractST {
    public static int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;
    public static String kafkaClientsPodName = "";
    public static String bridgeServiceName = KafkaBridgeResources.serviceName(CLUSTER_NAME);
    public static String bridgeUrl = "";

    public static String producerName = "bridge-producer";
    public static String consumerName = "bridge-consumer";

    protected WebClient client;

    void deployClusterOperator(String namespace) throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(namespace);
    }
}
