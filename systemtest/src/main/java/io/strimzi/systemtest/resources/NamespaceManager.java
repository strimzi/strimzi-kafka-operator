/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceStatus;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NamespaceManager {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceManager.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private static NamespaceManager instance;

    // {test-suite-name} -> {{namespace-1}, {namespace-2},...,}
    private final static Map<CollectorElement, Set<String>> MAP_WITH_SUITE_NAMESPACES = new HashMap<>();

    public static synchronized NamespaceManager getInstance() {
        if (instance == null) {
            instance = new NamespaceManager();
        }
        return instance;
    }

    private synchronized void addNamespaceToSet(String namespaceName, CollectorElement collectorElement) {
        if (MAP_WITH_SUITE_NAMESPACES.containsKey(collectorElement)) {
            Set<String> testSuiteNamespaces = MAP_WITH_SUITE_NAMESPACES.get(collectorElement);
            testSuiteNamespaces.add(namespaceName);
            MAP_WITH_SUITE_NAMESPACES.put(collectorElement, testSuiteNamespaces);
        } else {
            // test-suite is new
            MAP_WITH_SUITE_NAMESPACES.put(collectorElement, new HashSet<>(Set.of(namespaceName)));
        }

        LOGGER.trace("SUITE_NAMESPACE_MAP: {}", MAP_WITH_SUITE_NAMESPACES);
    }

    private synchronized void removeNamespaceFromSet(String namespaceName, CollectorElement collectorElement) {
        // dynamically removing from the map
        Set<String> testSuiteNamespaces = new HashSet<>(MAP_WITH_SUITE_NAMESPACES.get(collectorElement));
        testSuiteNamespaces.remove(namespaceName);

        MAP_WITH_SUITE_NAMESPACES.put(collectorElement, testSuiteNamespaces);

        LOGGER.trace("SUITE_NAMESPACE_MAP after deletion: {}", MAP_WITH_SUITE_NAMESPACES);
    }

    public void createNamespace(String namespaceName) {
        // in case we are restarting some testcase and the namespace already exists, we should delete it before creation
        if (shouldDeleteNamespace(namespaceName)) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespaceName);
            deleteNamespaceWithWait(namespaceName);
        }

        LOGGER.info("Creating Namespace: {}", namespaceName);
        kubeClient().createNamespace(namespaceName);
    }

    public void createNamespaceWithWait(String namespaceName) {
        createNamespace(namespaceName);
        waitForNamespaceCreation(namespaceName);
    }

    public void createNamespaceAndAddToSet(String namespaceName, CollectorElement collectorElement) {
        createNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            addNamespaceToSet(namespaceName, collectorElement);
        }
    }

    public void createNamespaceAndPrepare(String namespaceName) {
        createNamespaceAndPrepare(namespaceName, null);
    }

    public void createNamespaceAndPrepare(String namespaceName, CollectorElement collectorElement) {
        createNamespaceAndAddToSet(namespaceName, collectorElement);
        NetworkPolicyResource.applyDefaultNetworkPolicySettings(Collections.singletonList(namespaceName));
        StUtils.copyImagePullSecrets(namespaceName);
    }

    public void createNamespaces(String useNamespace, CollectorElement collectorElement, List<String> namespacesToBeCreated) {
        namespacesToBeCreated.forEach(namespaceToBeCreated -> createNamespaceAndPrepare(namespaceToBeCreated, collectorElement));

        KubeClusterResource.getInstance().setNamespace(useNamespace);
    }

    public void deleteNamespace(String namespaceName) {
        LOGGER.info("Deleting Namespace: {}", namespaceName);
        kubeClient().deleteNamespace(namespaceName);
    }

    public void deleteNamespaceWithWait(String namespaceName) {
        deleteNamespace(namespaceName);
        waitForNamespaceDeletion(namespaceName);
    }

    public void deleteNamespaceWithWaitAndRemoveFromSet(String namespaceName, CollectorElement collectorElement) {
        deleteNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            removeNamespaceFromSet(namespaceName, collectorElement);
        }
    }

    public void deleteAllNamespacesFromSet() {
        MAP_WITH_SUITE_NAMESPACES.values()
            .forEach(setOfNamespaces ->
                setOfNamespaces.parallelStream()
                    .forEach(this::deleteNamespaceWithWait));

        MAP_WITH_SUITE_NAMESPACES.clear();
    }

    private void waitForNamespaceDeletion(String namespaceName) {
        TestUtils.waitFor("Namespace: " + namespaceName + "to be deleted", TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT, () -> {
            Namespace namespace = kubeClient().getNamespace(namespaceName);

            if (namespace == null) {
                return true;
            } else if (isNamespaceDeletionStuckOnFinalizers(namespace.getStatus())) {
                LOGGER.debug("There are KafkaTopics with finalizers remaining in Namespace: {}, going to set those finalizers to null", namespaceName);
                KafkaTopicUtils.setFinalizersInAllTopicsToNull(namespaceName);
            }
            return false;
        });
    }

    private void waitForNamespaceCreation(String namespaceName) {
        TestUtils.waitFor("Namespace: " + namespaceName + "to be created", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> kubeClient().getNamespace(namespaceName) != null);
    }

    private boolean isNamespaceDeletionStuckOnFinalizers(NamespaceStatus namespaceStatus) {
        return namespaceStatus != null
            && namespaceStatus.getConditions() != null
            && namespaceStatus.getConditions().stream().anyMatch(condition -> condition.getReason().contains("SomeFinalizersRemain"));
    }

    private boolean shouldDeleteNamespace(String namespaceName) {
        return kubeClient().getNamespace(namespaceName) != null
            && !Environment.SKIP_TEARDOWN;
    }

    public static Map<CollectorElement, Set<String>> getMapWithSuiteNamespaces() {
        return MAP_WITH_SUITE_NAMESPACES;
    }
}
