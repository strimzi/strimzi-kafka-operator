/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaBridgeUtils {
    private KafkaBridgeUtils() {}

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceBridgeResourceInSpecificNamespace(String namespaceName, String resourceName, Consumer<KafkaBridge> editor) {
        ResourceManager.replaceCrdResource(namespaceName, KafkaBridge.class, KafkaBridgeList.class, resourceName, editor);
    }

    /**
     * Wait until KafkaBridge is in desired state
     * @param namespaceName Namespace name
     * @param clusterName name of KafkaBridge cluster
     * @param state desired state
     */
    public static boolean waitForKafkaBridgeStatus(String namespaceName, String clusterName, Enum<?> state) {
        KafkaBridge kafkaBridge = kafkaBridgeClient().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(namespaceName, kafkaBridgeClient(), kafkaBridge.getKind(),
            kafkaBridge.getMetadata().getName(), state, ResourceOperation.getTimeoutForResourceReadiness(kafkaBridge.getKind()));
    }

    public static boolean waitForKafkaBridgeReady(String namespaceName, String clusterName) {
        return waitForKafkaBridgeStatus(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaBridgeNotReady(final String namespaceName, String clusterName) {
        return waitForKafkaBridgeStatus(namespaceName, clusterName, NotReady);
    }
}
