/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;

/**
 * Base for test classes where HTTP Bridge is used.
 */
@ExtendWith(VertxExtension.class)
@Tag(BRIDGE)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
public class HttpBridgeAbstractST extends AbstractST {
    public static int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;
    public static String bridgeHost = "";
    public static String kafkaClientsPodName = "";
    public static String bridgeServiceName = KafkaBridgeResources.serviceName(CLUSTER_NAME);

    public static String producerName = "bridge-producer";
    public static String consumerName = "bridge-consumer";

    protected WebClient client;

    void deployClusterOperator(String namespace) throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(namespace);
    }
}
