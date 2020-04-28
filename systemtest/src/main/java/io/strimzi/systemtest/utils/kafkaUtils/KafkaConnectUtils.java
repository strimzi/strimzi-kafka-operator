/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectUtils.class);

    private KafkaConnectUtils() {}

    /**
     * Wait until the given Kafka Connect is in desired state.
     * @param clusterName name of KafkaConnect cluster
     * @param status desired state
     */
    public static void waitForConnectStatus(String clusterName, String status) {
        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
        ResourceManager.waitForResourceStatus(KafkaConnectResource.kafkaConnectClient(), kafkaConnect, status);
    }

    public static void waitForConnectReady(String clusterName) {
        waitForConnectStatus(clusterName, "Ready");
    }

    public static void waitForConnectNotReady(String clusterName) {
        waitForConnectStatus(clusterName, "NotReady");
    }

    public static void waitUntilKafkaConnectRestApiIsAvailable(String podNamePrefix) {
        LOGGER.info("Waiting until KafkaConnect API is available");
        TestUtils.waitFor("Waiting until KafkaConnect API is available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> cmdKubeClient().execInPod(podNamePrefix, "/bin/bash", "-c", "curl -I http://localhost:8083/connectors").out().contains("HTTP/1.1 200 OK\n"));
        LOGGER.info("KafkaConnect API is available");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String sinkFileName, String message) {
        LOGGER.info("Waiting for messages in file sink on {}", kafkaConnectPodName);
        TestUtils.waitFor("messages in file sink", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(message));
        LOGGER.info("Expected messages are in file sink on {}", kafkaConnectPodName);
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String sinkFileName) {
        waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, sinkFileName,
                "\"Sending messages\": \"Hello-world - 99\"");
    }
}
