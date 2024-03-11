/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class PersistentVolumeClaimUtils {

    private static final Logger LOGGER = LogManager.getLogger(PersistentVolumeClaimUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private PersistentVolumeClaimUtils() { }

    public static void waitUntilPVCLabelsChange(String namespaceName, String clusterName, Map<String, String> newLabels, String labelKey) {
        LOGGER.info("Waiting for PVC labels to change {}", newLabels.toString());
        TestUtils.waitFor("PVC labels to change -> " + newLabels.toString(), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                List<Boolean> allPvcsHasLabelsChanged =
                    kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream()
                        // filter specific pvc which belongs to cluster-name
                        .filter(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName))
                        // map each value if it is changed [False, True, True] etc.
                        .map(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getLabels().get(labelKey).equals(newLabels.get(labelKey)))
                        .collect(Collectors.toList());

                LOGGER.debug("Labels changed: {}", allPvcsHasLabelsChanged.toString());

                // all must be TRUE...
                return allPvcsHasLabelsChanged.size() > 0 && !allPvcsHasLabelsChanged.contains(Boolean.FALSE);
            });
        LOGGER.info("PVC labels changed {}", newLabels.toString());
    }

    public static void waitUntilPVCAnnotationChange(String namespaceName, String clusterName, Map<String, String> newAnnotation, String annotationKey) {
        LOGGER.info("Waiting for PVC annotation to change {}", newAnnotation.toString());
        TestUtils.waitFor("PVC labels to change -> " + newAnnotation.toString(), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                List<Boolean> allPvcsHasLabelsChanged =
                    kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream()
                        // filter specific pvc which belongs to cluster-name
                        .filter(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName))
                        // map each value if it is changed [False, True, True] etc.
                        .map(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getAnnotations().get(annotationKey).equals(newAnnotation.get(annotationKey)))
                        .collect(Collectors.toList());

                LOGGER.debug("Annotations changed: {}", allPvcsHasLabelsChanged.toString());

                // all must be TRUE...
                return allPvcsHasLabelsChanged.size() > 0 && !allPvcsHasLabelsChanged.contains(Boolean.FALSE);
            });
        LOGGER.info("PVC annotation changed {}", newAnnotation.toString());
    }

    public static void waitForPersistentVolumeClaimPhase(String persistentVolumeName, String wantedPhase) {
        TestUtils.waitFor("PV: " + persistentVolumeName + " to be in phase: " + wantedPhase, TestConstants.RECONCILIATION_INTERVAL, TestConstants.GLOBAL_TIMEOUT, () -> {
            String currentPhase = kubeClient().getPersistentVolumeWithName(persistentVolumeName).getStatus().getPhase();
            LOGGER.info("PV: {} is in phase: {}", persistentVolumeName, currentPhase);
            return currentPhase.equals(wantedPhase);
        });
    }

    public static void waitForPersistentVolumeClaimDeletion(String namespaceName, String pvcName) {
        TestUtils.waitFor("PVC deletion", TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, TestConstants.GLOBAL_TIMEOUT_SHORT, () -> {
            if (kubeClient().getPersistentVolumeClaim(namespaceName, pvcName) != null) {
                LOGGER.warn("PVC: {}/{} has not been deleted yet! Triggering force delete using cmd client!", namespaceName, pvcName);
                cmdKubeClient(namespaceName).deleteByName("pvc", pvcName);
                return false;
            }

            return true;
        });
    }

    public static void waitForPersistentVolumeClaimDeletion(TestStorage testStorage, int expectedNum) {
        LOGGER.info("Waiting for PVC(s): {}/{} to reach expected amount: {}", testStorage.getClusterName(), testStorage.getNamespaceName(), expectedNum);
        TestUtils.waitFor("PVC(s) to be created/deleted", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeClusterResource.kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                .filter(pvc -> pvc.getMetadata().getName().contains("data-") && pvc.getMetadata().getName().contains(testStorage.getBrokerComponentName())).collect(Collectors.toList()).size() == expectedNum
        );
    }

    public static void waitForJbodStorageDeletion(String namespaceName, int volumesCount, String clusterName, List<SingleVolumeStorage> volumes) {
        int numberOfPVCWhichShouldBeDeleted = volumes.stream().filter(
            singleVolumeStorage -> ((PersistentClaimStorage) singleVolumeStorage).isDeleteClaim()
        ).collect(Collectors.toList()).size();

        TestUtils.waitFor("JBOD storage deletion", TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Duration.ofMinutes(6).toMillis(), () -> {
            List<String> pvcs = kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream()
                .filter(pvc -> pvc.getMetadata().getName().contains(clusterName))
                .map(pvc -> pvc.getMetadata().getName())
                .collect(Collectors.toList());

            // pvcs must be deleted (1 storage -> 2 pvcs)
            return volumesCount - (numberOfPVCWhichShouldBeDeleted * 2) == pvcs.size();
        });
    }

    public static void deletePvcsByPrefixWithWait(String namespaceName, String prefix) {
        List<PersistentVolumeClaim> persistentVolumeClaimsList = kubeClient().listPersistentVolumeClaims(namespaceName, prefix);

        for (PersistentVolumeClaim persistentVolumeClaim : persistentVolumeClaimsList) {
            kubeClient().deletePersistentVolumeClaim(namespaceName, persistentVolumeClaim.getMetadata().getName());
        }

        for (PersistentVolumeClaim persistentVolumeClaim : persistentVolumeClaimsList) {
            waitForPersistentVolumeClaimDeletion(namespaceName, persistentVolumeClaim.getMetadata().getName());
        }
    }
}
