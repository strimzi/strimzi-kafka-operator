/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.templates.kubernetes.ServiceTemplates;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.crd.KafkaBridgeResource.kafkaBridgeClient;

public class KafkaBridgeUtils {
    private KafkaBridgeUtils() {}

    public static Service createBridgeNodePortService(String clusterName, String namespace, String serviceName) {
        Map<String, String> map = new HashMap<>();
        map.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        map.put(Labels.STRIMZI_KIND_LABEL, "KafkaBridge");
        map.put(Labels.STRIMZI_NAME_LABEL, clusterName + "-bridge");

        // Create node port service for expose bridge outside Kubernetes
        return ServiceTemplates.getSystemtestsServiceResource(serviceName, TestConstants.HTTP_BRIDGE_DEFAULT_PORT, namespace, "TCP")
                    .editSpec()
                        .withType("NodePort")
                        .withSelector(map)
                    .endSpec().build();
    }

    // ======================================= MESSAGE CHECKING ======================================================

    /**
     * Wait until KafkaBridge is in desired state
     * @param namespaceName Namespace name
     * @param clusterName name of KafkaBridge cluster
     * @param state desired state
     */
    public static boolean waitForKafkaBridgeStatus(String namespaceName, String clusterName, Enum<?> state) {
        KafkaBridge kafkaBridge = kafkaBridgeClient().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(kafkaBridgeClient(), kafkaBridge.getKind(), namespaceName,
            kafkaBridge.getMetadata().getName(), state, ResourceOperation.getTimeoutForResourceReadiness(kafkaBridge.getKind()));
    }

    public static boolean waitForKafkaBridgeReady(String namespaceName, String clusterName) {
        return waitForKafkaBridgeStatus(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaBridgeNotReady(final String namespaceName, String clusterName) {
        return waitForKafkaBridgeStatus(namespaceName, clusterName, NotReady);
    }
}
