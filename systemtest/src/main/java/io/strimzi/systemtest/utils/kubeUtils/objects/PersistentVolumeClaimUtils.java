/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PersistentVolumeClaimUtils {
    private static final Logger LOGGER = LogManager.getLogger(PersistentVolumeClaimUtils.class);

    /**
     * Private constructor to prevent instantiating.
     */
    private PersistentVolumeClaimUtils() { }

    /**
     * Returns filtered list of PVCs in specified Namespace with specified sub-String in their name.
     *
     * @param namespaceName     Namespace where the PVCs should be present.
     * @param substring         Sub-String that the names of PVCs contain.
     *
     * @return  filtered list of PVCs in specified Namespace with specified sub-String in their name.
     */
    public static List<PersistentVolumeClaim> listPVCsByNameSubstring(String namespaceName, String substring) {
        return KubeResourceManager.get().kubeClient().getClient().persistentVolumeClaims().inNamespace(namespaceName).list().getItems()
            .stream()
            .filter(persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(substring))
            .toList();
    }

    public static void waitUntilPVCLabelsChange(String namespaceName, String clusterName, Map<String, String> newLabels, String labelKey) {
        LOGGER.info("Waiting for PVC labels to change {}", newLabels.toString());
        TestUtils.waitFor("PVC labels to change -> " + newLabels.toString(), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                List<Boolean> allPvcsHasLabelsChanged =
                    listPVCsByNameSubstring(namespaceName, clusterName).stream()
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
                    listPVCsByNameSubstring(namespaceName, clusterName).stream()
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
            String currentPhase = KubeResourceManager.get().kubeClient().getClient().persistentVolumes().withName(persistentVolumeName).get().getStatus().getPhase();
            LOGGER.info("PV: {} is in phase: {}", persistentVolumeName, currentPhase);
            return currentPhase.equals(wantedPhase);
        });
    }

    public static void waitForPersistentVolumeClaimDeletion(String namespaceName, String pvcName) {
        TestUtils.waitFor("PVC deletion", TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, TestConstants.GLOBAL_TIMEOUT_SHORT, () -> {
            if (KubeResourceManager.get().kubeClient().getClient().persistentVolumeClaims().inNamespace(namespaceName).withName(pvcName).get() != null) {
                LOGGER.warn("PVC: {}/{} has not been deleted yet! Triggering force delete using cmd client!", namespaceName, pvcName);
                KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteByName("pvc", pvcName);
                return false;
            }
            return true;
        });
    }

    public static void waitForPersistentVolumeClaimDeletion(TestStorage testStorage, int expectedNum) {
        LOGGER.info("Waiting for PVC(s): {}/{} to reach expected amount: {}", testStorage.getClusterName(), testStorage.getNamespaceName(), expectedNum);
        TestUtils.waitFor("PVC(s) to be created/deleted", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT,
            () -> listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                .filter(pvc -> pvc.getMetadata().getName().contains("data-") && pvc.getMetadata().getName().contains(testStorage.getBrokerComponentName())).collect(Collectors.toList()).size() == expectedNum
        );
    }

    public static void waitForPvcCount(TestStorage testStorage, int expectedNum) {
        LOGGER.info("Waiting for PVC(s): {}/{} to reach expected amount: {}", testStorage.getClusterName(), testStorage.getNamespaceName(), expectedNum);
        TestUtils.waitFor("PVC(s) to be created/deleted", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT,
            () -> listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                .filter(pvc -> pvc.getMetadata().getName().contains("data-") && pvc.getMetadata().getName().contains(testStorage.getBrokerComponentName())).toList().size() == expectedNum
        );
    }

    public static void waitForJbodStorageDeletion(String namespaceName, int volumesCount, String clusterName, List<SingleVolumeStorage> volumes) {
        int numberOfPVCWhichShouldBeDeleted = volumes.stream().filter(
            singleVolumeStorage -> ((PersistentClaimStorage) singleVolumeStorage).isDeleteClaim()
        ).collect(Collectors.toList()).size();

        TestUtils.waitFor("JBOD storage deletion", TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Duration.ofMinutes(6).toMillis(), () -> {
            List<String> pvcs = listPVCsByNameSubstring(namespaceName, clusterName).stream()
                .filter(pvc -> pvc.getMetadata().getName().contains(clusterName))
                .map(pvc -> pvc.getMetadata().getName())
                .collect(Collectors.toList());

            // pvcs must be deleted (1 storage -> 2 pvcs)
            return volumesCount - (numberOfPVCWhichShouldBeDeleted * 2) == pvcs.size();
        });
    }

    public static void deletePvcsByPrefixWithWait(String namespaceName, String prefix) {
        List<PersistentVolumeClaim> persistentVolumeClaimsList = listPVCsByNameSubstring(namespaceName, prefix);

        for (PersistentVolumeClaim persistentVolumeClaim : persistentVolumeClaimsList) {
            KubeResourceManager.get().kubeClient().getClient()
                .persistentVolumeClaims().inNamespace(namespaceName).withName(persistentVolumeClaim.getMetadata().getName()).delete();
        }

        for (PersistentVolumeClaim persistentVolumeClaim : persistentVolumeClaimsList) {
            waitForPersistentVolumeClaimDeletion(namespaceName, persistentVolumeClaim.getMetadata().getName());
        }
    }

    /**
     * Waits until the size of specific Persistent Volume Claims (PVCs) changes to the expected size. This method continuously checks
     * the size of all PVCs that start with a given prefix within the specified namespace and cluster until all match the expected size or
     * until the global timeout is reached.
     *
     * @param testStorage                       The TestStorage instance containing the cluster name and namespace information,
     *                                          used to identify the cluster and namespace in which the PVCs are managed.
     * @param pvcPrefixName                     The prefix of the PVC names to filter the PVCs that need to be checked.
     *                                          Only PVCs whose names start with this prefix will be considered.
     * @param expectedSize                      The expected size to which the PVC should change, specified in a format
     *                                          understood by Kubernetes, e.g., "10Gi". This is the size
     *                                          that all matching PVCs must reach for the method to stop waiting and return successfully.
     * @throws io.strimzi.test.WaitException    if the timeout is reached before all PVCs match the expected size.
     */
    public static void waitUntilSpecificPvcSizeChange(final TestStorage testStorage, final String pvcPrefixName, final String expectedSize) {
        TestUtils.waitFor("size change of PVCs matching " + pvcPrefixName + " to " + expectedSize,
            TestConstants.GLOBAL_POLL_INTERVAL,
            TestConstants.GLOBAL_TIMEOUT,
            () -> {
                final List<PersistentVolumeClaim> pvcs = listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                    .filter(pvc -> pvc.getMetadata().getName().startsWith(pvcPrefixName)).toList();

                if (pvcs.isEmpty()) {
                    LOGGER.warn("No PVCs found by the prefix: {}", pvcPrefixName);
                    return false;
                }

                // Check if all PVCs match the expected size
                for (final PersistentVolumeClaim pvc : pvcs) {
                    final String currentSize = pvc.getSpec().getResources().getRequests().get("storage").toString();
                    LOGGER.debug("Current size of PVC {}: {}", pvc.getMetadata().getName(), currentSize);
                    if (!currentSize.equals(expectedSize)) {
                        LOGGER.info("PVC {} size {} does not match expected size {}", pvc.getMetadata().getName(), currentSize, expectedSize);
                        return false;
                    }
                }
                return true;
            });
        LOGGER.info("Size of all matched PVC(s) successfully changed to {}", expectedSize);
    }
}
