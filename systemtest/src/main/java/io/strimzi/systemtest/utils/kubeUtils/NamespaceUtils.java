/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.NamespaceStatus;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

/**
 * Class containing helper methods for easier work with Namespaces.
 */
public class NamespaceUtils {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceUtils.class);

    /**
     * Takes the Namespaces from {@param namespacesToBeCreated} and runs {@link #createNamespaceAndPrepare(String)}
     * method for each.
     *
     * @param namespacesToBeCreated     Namespaces that should be created and prepared.
     */
    public static void createNamespacesAndPrepare(String... namespacesToBeCreated) {
        for (String namespaceName : namespacesToBeCreated) {
            createNamespaceAndPrepare(namespaceName);
        }
    }

    /**
     * Method for creating and preparing Namespace with specified name.
     * It firstly checks if the Namespace is created already, if yes, it is deleted.
     * Otherwise, it creates the Namespace using {@link KubeResourceManager} and then
     * applies default NetworkPolicies and copies the image pull Secret if needed.
     *
     * @param namespaceName     Name of the Namespace that should be created and prepared.
     */
    public static void createNamespaceAndPrepare(String namespaceName) {
        if (KubeResourceManager.get().kubeClient().namespaceExists(namespaceName)) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespaceName);
            deleteNamespace(namespaceName);
        }

        LOGGER.info("Creating Namespace: {}", namespaceName);
        KubeResourceManager.get().createResourceWithWait(new NamespaceBuilder()
            .withNewMetadata()
                .withName(namespaceName)
            .endMetadata()
            .build()
        );

        NetworkPolicyUtils.applyDefaultNetworkPolicySettings(Collections.singletonList(namespaceName));
        StUtils.copyImagePullSecrets(namespaceName);
    }

    /**
     * Based on the specified {@param namespaceName}, it gets the {@link Namespace}
     * and deletes it using {@link KubeResourceManager}.
     *
     * @param namespaceName     Name of the Namespace that should be deleted.
     */
    public static void deleteNamespace(String namespaceName) {
        Namespace nsToBeDeleted = KubeResourceManager.get().kubeClient().getClient().namespaces().withName(namespaceName).get();

        if (nsToBeDeleted != null) {
            KubeResourceManager.get().deleteResourceWithWait(nsToBeDeleted);
        }
    }

    /**
     * Checks if Namespace is stuck on finalizers or not.
     *
     * @param namespaceStatus Status of Namespace
     * @return {@code boolean} value determining if the Namespace is stuck on finalizers or not
     */
    public static boolean isNamespaceDeletionStuckOnFinalizers(NamespaceStatus namespaceStatus) {
        return namespaceStatus != null
            && namespaceStatus.getConditions() != null
            && namespaceStatus.getConditions().stream().anyMatch(condition -> condition.getReason().contains("SomeFinalizersRemain"));
    }
}
