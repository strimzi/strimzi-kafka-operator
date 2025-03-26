/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.crds;

import io.skodjob.testframe.resources.KubeResourceManager;

public class CrdUtils {

    /**
     * Checks if a given Custom Resource Definition (CRD) is present in the Kubernetes cluster.
     * This method constructs the fully qualified name of the CRD using its resource plural name
     * and API group.
     *
     * @param resourcePluralName  The plural name of the custom resource (e.g., "kafkatopics").
     * @param resourceApiGroupName The API group name of the custom resource (e.g., "kafka.strimzi.io").
     * @return {@code true} if the CRD is present in the cluster, {@code false} otherwise.
     */
    public static boolean isCrdPresent(final String resourcePluralName, final String resourceApiGroupName) {
        final String fullyQualifiedCrdName = resourcePluralName + "." + resourceApiGroupName;
        return KubeResourceManager.get().kubeCmdClient().exec(false, "get", "crd", fullyQualifiedCrdName).returnCode() == 0;
    }
}
