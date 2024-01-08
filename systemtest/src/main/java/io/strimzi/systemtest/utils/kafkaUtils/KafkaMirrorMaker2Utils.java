/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.test.TestUtils;

import java.util.Map;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaMirrorMaker2Utils {

    private KafkaMirrorMaker2Utils() {}

    /**
     * Wait for KafkaMirrorMaker2 to be in desired state
     * @param namespaceName name of the namespace
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static boolean waitForKafkaMirrorMaker2Status(String namespaceName, String clusterName, Enum<?> state) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client(), kafkaMirrorMaker2, state);
    }

    /**
     * Wait for KafkaMirrorMaker2 to be in desired state
     * @param namespaceName name of the namespace
     * @param clusterName name of KafkaMirrorMaker2 cluster
     */
    public static boolean waitForKafkaMirrorMaker2Ready(String namespaceName, String clusterName) {
        return waitForKafkaMirrorMaker2Status(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaMirrorMaker2NotReady(final String namespaceName, String clusterName) {
        return waitForKafkaMirrorMaker2Status(namespaceName, clusterName, NotReady);
    }

    @SuppressWarnings("unchecked")
    public static void waitForKafkaMirrorMaker2ConnectorReadiness(String namespaceName, String clusterName) {
        TestUtils.waitFor("MirrorMaker2 connectors readiness", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT, () -> {
            KafkaMirrorMaker2Status kafkaMirrorMaker2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus();
            // There should be only three connectors in the status of MM2
            if (kafkaMirrorMaker2Status.getConnectors().size() != 3) {
                return false;
            }
            for (Map<String, Object> connector : kafkaMirrorMaker2Status.getConnectors()) {
                Map<String, String> status = (Map<String, String>) connector.get("connector");
                if (!status.get("state").equals("RUNNING")) {
                    return false;
                }
            }
            return true;
        });
    }

    public static boolean waitForKafkaMirrorMaker2StatusMessage(String namespaceName, String clusterName, String message) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatusMessage(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client(), kafkaMirrorMaker2, message);
    }
}
