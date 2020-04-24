/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.resources.ResourceManager.logCurrentStatus;
import static io.strimzi.systemtest.resources.crd.KafkaConnectorResource.kafkaConnectorClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectorUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectorUtils.class);

    private KafkaConnectorUtils() {}

    /**
     * WaitForStabilityConnector method, verifying stability of connector
     * @param connectorName connector name
     * @param connectPodName connects2i or connect pod name
     */
    public static void waitForConnectorStability(String connectorName, String connectPodName) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Waiting for stability of connector " + connectorName, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String availableConnectors = getCreatedConnectors(connectPodName);
                if (availableConnectors.contains(connectorName)) {
                    LOGGER.info("Connector with name {} is present. Remaining seconds for stability {}", connectorName,
                            Constants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == (Constants.GLOBAL_RECONCILIATION_COUNT);
                } else {
                    throw new RuntimeException("Connector" + connectorName + " is not stable!");
                }
            }, () -> logCurrentStatus(kafkaConnectorClient().inNamespace(kubeClient().getNamespace()).withName(connectorName).get())
        );
    }

    /**
     * Wait until KafkaConnector is in desired state
     * @param connectorName name of KafkaConnector
     * @param state desired state
     */
    public static void waitForConnectorStatus(String name, String state) {
        KafkaConnector kafkaConnector = kafkaConnectorClient().inNamespace(kubeClient().getNamespace()).withName(name).get();
        ResourceManager.waitForStatus(kafkaConnectorClient(), kafkaConnector, state);
    }

    public static void waitForConnectorReady(String connectorName) {
        waitForConnectorStatus(connectorName, "Ready");
    }

    public static void waitForConnectorNotReady(String connectorName) {
        waitForConnectorStatus(connectorName, "NotReady");
    }

    public static String getCreatedConnectors(String connectPodName) {
        return cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c",
                "curl -X GET http://localhost:8083/connectors"
        ).out();
    }

    public static void waitForConnectorCreation(String connectS2IPodName, String connectorName) {
        TestUtils.waitFor(connectorName + " connector creation", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            String availableConnectors = getCreatedConnectors(connectS2IPodName);
            return availableConnectors.contains(connectorName);
        }, () -> logCurrentStatus(kafkaConnectorClient().inNamespace(kubeClient().getNamespace()).withName(connectorName).get()));
    }
}
