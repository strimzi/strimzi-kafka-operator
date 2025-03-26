/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.CrdClients.kafkaMirrorMaker2Client;

public class KafkaMirrorMaker2Utils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2Utils.class);

    private KafkaMirrorMaker2Utils() {}

    /**
     * Replaces KafkaMirrorMaker2 in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName     name of the Namespace where the resource should be replaced.
     * @param resourceName      name of the KafkaMirrorMaker2's name.
     * @param editor            editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<KafkaMirrorMaker2> editor) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(resourceName).get();
        KubeResourceManager.get().replaceResourceWithRetries(kafkaMirrorMaker2, editor);
    }

    /**
     * Wait for KafkaMirrorMaker2 to be in desired state
     * @param namespaceName name of the namespace
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static boolean waitForKafkaMirrorMaker2Status(String namespaceName, String clusterName, Enum<?> state) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get();
        return KubeResourceManager.get().waitResourceCondition(kafkaMirrorMaker2, ResourceConditions.resourceHasDesiredState(state), ResourceOperation.getTimeoutForResourceReadiness(kafkaMirrorMaker2.getKind()));
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
            KafkaMirrorMaker2Status kafkaMirrorMaker2Status = kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus();
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

    public static void waitForKafkaMirrorMaker2StatusMessage(String namespaceName, String clusterName, String message) {
        LOGGER.info("Waiting for {}: {}/{} will contain desired status message: {}",
            KafkaMirrorMaker2.RESOURCE_KIND, namespaceName, clusterName, message);

        TestUtils.waitFor(String.format("%s: %s#%s will contain desired status message: %s", KafkaMirrorMaker2.RESOURCE_KIND, namespaceName, clusterName, message),
            TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, ResourceOperation.getTimeoutForResourceReadiness(KafkaMirrorMaker2.RESOURCE_KIND),
            () -> kafkaMirrorMaker2Client()
                .inNamespace(namespaceName)
                .withName(clusterName)
                .get()
                .getStatus()
                .getConditions()
                .stream()
                .anyMatch(condition -> condition.getMessage().contains(message) && condition.getStatus().equals("True"))
        );

        LOGGER.info("{}: {}/{} contains desired message in status: {}", KafkaMirrorMaker2.RESOURCE_KIND, namespaceName, clusterName, message);
    }
}
