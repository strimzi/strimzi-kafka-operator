/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectS2IUtils {

    private KafkaConnectS2IUtils() {}

    /**
     * Wait until the given Kafka ConnectS2I cluster is in desired state.
     * @param clusterName The name of the Kafka ConnectS2I cluster.
     * @param status desired status value
     */
    // Deprecation is suppressed because of KafkaConnectS2I
    @SuppressWarnings("deprecation")
    public static boolean waitForConnectS2IStatus(String clusterName, Enum<?>  status) {
        KafkaConnectS2I kafkaConnectS2I = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaConnectS2IResource.kafkaConnectS2IClient(), kafkaConnectS2I, status);
    }

    public static boolean waitForConnectS2IReady(String clusterName) {
        return waitForConnectS2IStatus(clusterName, Ready);
    }

    public static boolean waitForConnectS2INotReady(String clusterName) {
        return waitForConnectS2IStatus(clusterName, NotReady);
    }
}
