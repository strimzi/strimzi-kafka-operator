/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base for test classes where HTTP Bridge is used.
 */
public class HttpBridgeAbstractST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeKafkaExternalListenersST.class);

    public static int bridgePort = Constants.HTTP_BRIDGE_DEFAULT_PORT;
    protected static final String CLUSTER_NAME = "http-bridge-cluster-name";
}
