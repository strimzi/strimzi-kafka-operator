/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.CrdClients.kafkaBridgeClient;

public class KafkaBridgeUtils {
    private KafkaBridgeUtils() {}

    /**
     * Replaces KafkaBridge in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName     name of the Namespace where the resource should be replaced.
     * @param resourceName      name of the KafkaBridge's name.
     * @param editor            editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<KafkaBridge> editor) {
        KafkaBridge kafkaBridge = kafkaBridgeClient().inNamespace(namespaceName).withName(resourceName).get();
        KubeResourceManager.get().replaceResourceWithRetries(kafkaBridge, editor);
    }

    /**
     * Wait until KafkaBridge is in desired state
     * @param namespaceName Namespace name
     * @param clusterName name of KafkaBridge cluster
     * @param state desired state
     */
    public static void waitForKafkaBridgeStatus(String namespaceName, String clusterName, Enum<?> state) {
        KafkaBridge kafkaBridge = kafkaBridgeClient().inNamespace(namespaceName).withName(clusterName).get();
        KubeResourceManager.get().waitResourceCondition(kafkaBridge, ResourceConditions.resourceHasDesiredState(state), ResourceOperation.getTimeoutForResourceReadiness(kafkaBridge.getKind()));
    }

    public static void waitForKafkaBridgeReady(String namespaceName, String clusterName) {
        waitForKafkaBridgeStatus(namespaceName, clusterName, Ready);
    }

    public static void waitForKafkaBridgeNotReady(final String namespaceName, String clusterName) {
        waitForKafkaBridgeStatus(namespaceName, clusterName, NotReady);
    }
}
