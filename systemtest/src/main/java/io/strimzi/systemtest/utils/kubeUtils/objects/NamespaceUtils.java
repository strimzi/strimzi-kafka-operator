/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.Namespace;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NamespaceUtils {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private NamespaceUtils() { }

    public static void waitForNamespaceDeletion(String name) {
        LOGGER.info("Waiting for Namespace: {} deletion", name);

        TestUtils.waitFor("Namespace: " + name, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> kubeClient().getNamespace(name) == null);
        LOGGER.info("Namespace: {} was deleted", name);
    }

    /**
     * Method for Namespace deletion with wait
     * In case that Namespace is stuck in `Terminating` state (due to finalizers in KafkaTopics), this method
     * removes these topics, to unblock the Namespace deletion
     * @param namespaceName name of the Namespace that should be deleted
     */
    public static void deleteNamespaceWithWait(String namespaceName) {
        LOGGER.info("Deleting Namespace: {}", namespaceName);

        kubeClient().deleteNamespace(namespaceName);

        TestUtils.waitFor("Namespace: " + namespaceName + "to be deleted", TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT, () -> {
            Namespace namespace = kubeClient().getNamespace(namespaceName);

            if (namespace == null) {
                return true;
            } else if (namespace.getStatus() != null && namespace.getStatus().getConditions() != null) {
                if (namespace.getStatus().getConditions().stream().anyMatch(condition -> condition.getReason().contains("SomeFinalizersRemain"))) {
                    LOGGER.debug("There are KafkaTopics with finalizers remaining in Namespace: {}, going to set those finalizers to null", namespaceName);
                    KafkaTopicUtils.setFinalizersInAllTopicsToNull(namespaceName);
                }
            }
            return false;
        });
    }
}
