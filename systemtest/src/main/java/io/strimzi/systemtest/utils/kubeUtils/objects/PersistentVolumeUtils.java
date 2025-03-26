/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.skodjob.testframe.resources.KubeResourceManager;

import java.util.List;

public class PersistentVolumeUtils {
    /**
     * Private constructor to prevent instantiating.
     */
    private PersistentVolumeUtils() { }

    /**
     * Returns list of {@link PersistentVolume} that are claimed for Namespace and the claimer.
     *
     * @param namespaceName     Name of the Namespace by which is the PV claimed.
     * @param claimer           Name of the claimer of the PV.
     *
     * @return  list of {@link PersistentVolume} that are claimed for Namespace and the claimer.
     */
    public static List<PersistentVolume> listClaimedPVs(String namespaceName, String claimer) {
        return KubeResourceManager.get().kubeClient().getClient().persistentVolumes().list().getItems().stream()
            .filter(pv -> {
                boolean containsClusterName = pv.getSpec().getClaimRef().getName().contains(claimer);
                boolean containsClusterNamespace = pv.getSpec().getClaimRef().getNamespace().contains(namespaceName);
                return containsClusterName && containsClusterNamespace;
            })
            .toList();
    }
}
