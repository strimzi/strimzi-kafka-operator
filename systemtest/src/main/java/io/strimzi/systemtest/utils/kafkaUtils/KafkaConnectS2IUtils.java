/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource.kafkaConnectS2IClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectS2IUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectS2IUtils.class);

    private KafkaConnectS2IUtils() {}

    /**
     * Wait until the given Kafka ConnectS2I cluster is in desired state.
     * @param clusterName The name of the Kafka ConnectS2I cluster.
     * @param status desired status value
     */
    public static void waitForConnectS2IStatus(String name, String status) {
        KafkaConnectS2I kafkaConnectS2I = kafkaConnectS2IClient().inNamespace(kubeClient().getNamespace()).withName(name).get();
        ResourceManager.waitForStatus(kafkaConnectS2IClient(), kafkaConnectS2I, status);
    }

    public static void waitForConnectS2IReady(String clusterName) {
        waitForConnectS2IStatus(clusterName, "Ready");
    }

    public static void waitForConnectS2INotReady(String clusterName) {
        waitForConnectS2IStatus(clusterName, "NotReady");
    }
}
