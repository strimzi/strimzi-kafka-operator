/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectS2IUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectS2IUtils.class);

    private KafkaConnectS2IUtils() {}

    /**
     * Wait until the given Kafka Connect S2I cluster is in desired state.
     * @param name The name of the Kafka Connect S2I cluster.
     * @param status desired status value
     */
    public static void waitForConnectS2IStatus(String name, String status) {
        LOGGER.info("Waiting for Kafka Connect S2I {} state: {}", name, status);
        TestUtils.waitFor("Kafka Connect S2I " + name + " state: " + status, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(kubeClient().getNamespace())
                    .withName(name).get().getStatus().getConditions().get(0).getType().equals(status),
            () -> StUtils.logCurrentStatus(KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(kubeClient().getNamespace()).withName(name).get()));
        LOGGER.info("Kafka Connect S2I {} is in desired state: {}", name, status);
    }

    public static void waitForRebalancingDone(String name) {
        LOGGER.info("Waiting for Kafka Connect S2I {} to rebalance", name);
        TestUtils.waitFor("Kafka Connect S2I rebalancing", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> {
                String connect = kubeClient().listPodNames("strimzi.io/kind", "KafkaConnectS2I").get(0);
                String log = kubeClient().logs(connect);
                // wait for second occurrence of message about finished rebalancing
                return (log.length() - log.replace("Finished starting connectors and tasks", "").length()) / "Finished starting connectors and tasks".length() == 2;
            });
        LOGGER.info("Kafka Connect S2I {} rebalanced", name);
    }
}
