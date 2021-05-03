/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.test.TestUtils;

import java.util.Map;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaMirrorMaker2Utils {

    private KafkaMirrorMaker2Utils() {}

    /**
     * Wait until KafkaMirrorMaker2 will be in desired state
     * @param namespaceName name of the namespace
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static boolean waitForKafkaMirrorMaker2Status(String namespaceName, String clusterName, Enum<?>  state) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client(), kafkaMirrorMaker2, state);
    }

    /**
     * Wait until KafkaMirrorMaker2 will be in desired state
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static boolean waitForKafkaMirrorMaker2Status(String clusterName, Enum<?>  state) {
        return waitForKafkaMirrorMaker2Status(kubeClient().getNamespace(), clusterName, state);
    }

    public static boolean waitForKafkaMirrorMaker2Ready(String namespaceName, String clusterName) {
        return waitForKafkaMirrorMaker2Status(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaMirrorMaker2Ready(String clusterName) {
        return waitForKafkaMirrorMaker2Status(clusterName, Ready);
    }

    public static boolean waitForKafkaMirrorMaker2NotReady(String clusterName) {
        return waitForKafkaMirrorMaker2Status(clusterName, NotReady);
    }

    public static void waitForKafkaMirrorMaker2ConnectorReadiness(String namespaceName, String clusterName) {
        TestUtils.waitFor("KafkaMirrorMaker2 connectors readiness", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            KafkaMirrorMaker2Status kafkaMirrorMaker2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus();
            for (Map<String, Object> connector : kafkaMirrorMaker2Status.getConnectors()) {
                if (!((Map<String, String>) connector.get("connector")).get("state").equals("RUNNING")) {
                    return false;
                }
            }
            return true;
        });
    }
}
