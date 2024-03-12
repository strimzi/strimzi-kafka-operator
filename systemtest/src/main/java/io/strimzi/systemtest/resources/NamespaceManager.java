/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
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

/**
 * Class for managing Namespaces during test execution.
 * It collects all Namespace related methods on one place, which removes duplicates that were originally across classes and
 * ensures, that all Namespaces will be handled in the same way
 */
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

    /**
     * Adds Namespace with {@param namespaceName} to the {@code MAP_WITH_SUITE_NAMESPACES} based on the {@param collectorElement}
     * The Map of these Namespaces is then used in {@link io.strimzi.systemtest.logs.LogCollector} for logs collection or in
     * {@code AbstractST.afterAllMayOverride} method for deleting all Namespaces after all test cases
     *
     * @param namespaceName name of the Namespace that should be added into the Set
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
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

    /**
     * Removes Namespace with {@param namespaceName} from the {@link #MAP_WITH_SUITE_NAMESPACES} based on the {@param collectorElement}.
     * After the Namespace is deleted, it is removed also from the Set -> so we know that we should not collect logs from there and that
     * everything should be cleared after a test case
     *
     * @param namespaceName name of the Namespace that should be added into the Set
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
    private synchronized void removeNamespaceFromSet(String namespaceName, CollectorElement collectorElement) {
        // dynamically removing from the map
        Set<String> testSuiteNamespaces = new HashSet<>(MAP_WITH_SUITE_NAMESPACES.get(collectorElement));
        testSuiteNamespaces.remove(namespaceName);

        MAP_WITH_SUITE_NAMESPACES.put(collectorElement, testSuiteNamespaces);

        LOGGER.trace("SUITE_NAMESPACE_MAP after deletion: {}", MAP_WITH_SUITE_NAMESPACES);
    }

    /**
     * Creates new Namespace with {@param namespaceName}
     * In case that the Namespace is already created and the {@link Environment#SKIP_TEARDOWN} is false, it firstly removes
     * the Namespace using {@link #deleteNamespaceWithWait(String)} and then creates a new one
     *
     * @param namespaceName name of the Namespace that should be created
     */
    public void createNamespace(String namespaceName) {
        // in case we are restarting some testcase and the namespace already exists, we should delete it before creation
        if (shouldDeleteNamespace(namespaceName)) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespaceName);
            deleteNamespaceWithWait(namespaceName);
        }

        LOGGER.info("Creating Namespace: {}", namespaceName);
        kubeClient().createNamespace(namespaceName);
    }

    /**
     * Encapsulates two methods - {@link #createNamespace(String)} and {@link #waitForNamespaceCreation(String)}.
     * It firstly creates new Namespace based on {@param namespaceName} and then waits for its creation
     *
     * @param namespaceName name of the Namespace that should be created0
     */
    public void createNamespaceWithWait(String namespaceName) {
        createNamespace(namespaceName);
        waitForNamespaceCreation(namespaceName);
    }

    /**
     * Method for creating Namespace with {@param namespaceName}, waiting for its creation, and adding it
     * to the {@link #MAP_WITH_SUITE_NAMESPACES}.
     * The last step is done only in case that {@param collectorElement} is not {@code null}
     *
     * @param namespaceName name of Namespace that should be created and added to the Set
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
    public void createNamespaceAndAddToSet(String namespaceName, CollectorElement collectorElement) {
        createNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            addNamespaceToSet(namespaceName, collectorElement);
        }
    }

    /**
     * Overloads {@link #createNamespaceAndPrepare(String, CollectorElement)} - with {@code CollectorElement} set to {@code null},
     * so the Namespace will not be added into the {@link #MAP_WITH_SUITE_NAMESPACES}.
     *
     * @param namespaceName name of Namespace that should be created
     */
    public void createNamespaceAndPrepare(String namespaceName) {
        createNamespaceAndPrepare(namespaceName, null);
    }

    /**
     * Method does following:
     *  - creates Namespace and waits for its readiness
     *  - adds the Namespace into {@link #MAP_WITH_SUITE_NAMESPACES} - based on {@param collectorElement}
     *  - applies default NetworkPolicy settings
     *  - copies image pull secrets from `default` Namespace
     *
     * @param namespaceName name of the Namespace that should be created and prepared
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
    public void createNamespaceAndPrepare(String namespaceName, CollectorElement collectorElement) {
        createNamespaceAndAddToSet(namespaceName, collectorElement);
        NetworkPolicyResource.applyDefaultNetworkPolicySettings(Collections.singletonList(namespaceName));
        StUtils.copyImagePullSecrets(namespaceName);
    }

    /**
     * Creates and prepares all Namespaces from {@param namespacesToBeCreated}.
     * After that sets Namespace in {@link KubeClusterResource} to {@param useNamespace}.
     *
     * @param useNamespace Namespace name that should be used in {@link KubeClusterResource}
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     * @param namespacesToBeCreated list of Namespaces that should be created
     */
    public void createNamespaces(String useNamespace, CollectorElement collectorElement, List<String> namespacesToBeCreated) {
        namespacesToBeCreated.forEach(namespaceToBeCreated -> createNamespaceAndPrepare(namespaceToBeCreated, collectorElement));

        KubeClusterResource.getInstance().setNamespace(useNamespace);
    }

    /**
     * Deletes Namespace with {@param namespaceName}
     *
     * @param namespaceName name of Namespace that should be deleted
     */
    public void deleteNamespace(String namespaceName) {
        LOGGER.info("Deleting Namespace: {}", namespaceName);
        kubeClient().deleteNamespace(namespaceName);
    }

    /**
     * Deletes Namespace with {@param namespaceName} and waits for its deletion.
     * It encapsulates two methods - {@link #deleteNamespace(String)} and {@link #waitForNamespaceDeletion(String)}.
     *
     * @param namespaceName name of Namespace that should be deleted
     */
    public void deleteNamespaceWithWait(String namespaceName) {
        deleteNamespace(namespaceName);
        waitForNamespaceDeletion(namespaceName);
    }

    /**
     * Deletes Namespace with {@param namespaceName}, waits for its deletion, and in case that {@param collectorElement}
     * is not {@code null}, removes the Namespace from the {@link #MAP_WITH_SUITE_NAMESPACES}.
     *
     * @param namespaceName
     * @param collectorElement
     */
    public void deleteNamespaceWithWaitAndRemoveFromSet(String namespaceName, CollectorElement collectorElement) {
        deleteNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            removeNamespaceFromSet(namespaceName, collectorElement);
        }
    }

    /**
     * For all entries inside the {@link #MAP_WITH_SUITE_NAMESPACES} it deletes all Namespaces in the particular Set
     * After that, it clears the whole Map
     * It is used mainly in {@code AbstractST.afterAllMayOverride} to remove everything after all test cases are executed
     */
    public void deleteAllNamespacesFromSet() {
        MAP_WITH_SUITE_NAMESPACES.values()
            .forEach(setOfNamespaces ->
                setOfNamespaces.parallelStream()
                    .forEach(this::deleteNamespaceWithWait));

        MAP_WITH_SUITE_NAMESPACES.clear();
    }

    /**
     * Waits for the Namespace with {@param namespaceName} to be deleted.
     * In case that the Namespace is stuck in `Terminating` state (and because of finalizers), it sets the finalizers of
     * all KafkaTopics to {@code null} - using {@link KafkaTopicUtils#setFinalizersInAllTopicsToNull(String)}.
     * The check if the Namespace is stuck is done by {@link #isNamespaceDeletionStuckOnFinalizers(NamespaceStatus)}
     * Finalizers are added to KafkaTopics using UTO and in case that we don't remove all the KafkaTopics before deletion of UTO,
     * we can simply get into the situation of hanging KafkaTopics.
     *
     * @param namespaceName name of the Namespace that should be deleted
     */
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

    /**
     * Waits for Namespace with {@param namespaceName} to be created.
     *
     * @param namespaceName name of Namespace that should be created
     */
    private void waitForNamespaceCreation(String namespaceName) {
        TestUtils.waitFor("Namespace: " + namespaceName + "to be created", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> kubeClient().getNamespace(namespaceName) != null);
    }

    /**
     * Checks if Namespace is stuck on finalizers or not.
     *
     * @param namespaceStatus Status of Namespace
     * @return {@code boolean} value determining if the Namespace is stuck on finalizers or not
     */
    private boolean isNamespaceDeletionStuckOnFinalizers(NamespaceStatus namespaceStatus) {
        return namespaceStatus != null
            && namespaceStatus.getConditions() != null
            && namespaceStatus.getConditions().stream().anyMatch(condition -> condition.getReason().contains("SomeFinalizersRemain"));
    }

    /**
     * Checks if the Namespace is already created and if the {@link Environment#SKIP_TEARDOWN} is false.
     * Based on that it returns value representing choice if the Namespace should be deleted or not.
     *
     * @param namespaceName name of Namespace that it should check for existence
     * @return {@code boolean} value determining if the Namespace should be deleted or not
     */
    private boolean shouldDeleteNamespace(String namespaceName) {
        return kubeClient().getNamespace(namespaceName) != null
            && !Environment.SKIP_TEARDOWN;
    }

    /**
     * @return {@link #MAP_WITH_SUITE_NAMESPACES}
     */
    public static Map<CollectorElement, Set<String>> getMapWithSuiteNamespaces() {
        return MAP_WITH_SUITE_NAMESPACES;
    }

    public static void labelNamespace(String namespaceName, Map<String, String> labels) {
        TestUtils.waitFor(
            String.join("%s will be updated with %s and the labels will be present", namespaceName, labels.toString()),
            TestConstants.GLOBAL_POLL_INTERVAL,
            TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    kubeClient().getClient().namespaces().withName(namespaceName).edit(namespace ->
                        new NamespaceBuilder(namespace)
                            .editOrNewMetadata()
                            .addToLabels("pod-security.kubernetes.io/enforce", "restricted")
                            .endMetadata()
                            .build()
                    );
                } catch (Exception e) {
                    LOGGER.warn("Failed to put labels to Namespace: {} due to {}, trying again", namespaceName, e.getMessage());
                    return false;
                }

                Namespace namespace = kubeClient().getNamespace(namespaceName);

                if (namespace != null) {
                    Map<String, String> namespaceLabels = namespace.getMetadata().getLabels();

                    return namespaceLabels.entrySet().containsAll(labels.entrySet());
                }

                return false;
            });
    }
}
