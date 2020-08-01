/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class PersistentVolumeClaimUtils {

    private static final Logger LOGGER = LogManager.getLogger(PersistentVolumeClaimUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private PersistentVolumeClaimUtils() { }

    public static void waitUntilPVCLabelsChange(Map<String, String> newLabels, String labelKey) {
        LOGGER.info("Wait until PVC labels will change {}", newLabels.toString());
        TestUtils.waitFor("PVC labels will change {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                for (PersistentVolumeClaim pvc : kubeClient().listPersistentVolumeClaims()) {
                    if (!pvc.getMetadata().getLabels().get(labelKey).equals(newLabels.get(labelKey))) {
                        return false;
                    }
                }
                return true;
            });
        LOGGER.info("PVC labels has changed {}", newLabels.toString());
    }

    public static void waitUntilPVCAnnotationChange(Map<String, String> newAnnotation, String annotationKey) {
        LOGGER.info("Wait until PVC annotation will change {}", newAnnotation.toString());
        TestUtils.waitFor("PVC labels will change {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                for (PersistentVolumeClaim pvc : kubeClient().listPersistentVolumeClaims()) {
                    if (!pvc.getMetadata().getLabels().get(annotationKey).equals(newAnnotation.get(annotationKey))) {
                        return false;
                    }
                }
                return true;
            });
        LOGGER.info("PVC annotation has changed {}", newAnnotation.toString());
    }

    public static void waitUntilPVCDeletion(String clusterName) {
        LOGGER.info("Wait until PVC deletion for cluster {}", clusterName);
        TestUtils.waitFor("PVC will be deleted {}", Constants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> {
                List<PersistentVolumeClaim> pvcList = kubeClient().listPersistentVolumeClaims().stream().filter(pvc -> pvc.getMetadata().getName().contains(clusterName)).collect(Collectors.toList());
                if (pvcList.isEmpty()) {
                    return true;
                } else {
                    for (PersistentVolumeClaim pvc : pvcList) {
                        LOGGER.warn("PVC {} is not deleted yet! Triggering force delete by cmd client!", pvc.getMetadata().getName());
                        cmdKubeClient().deleteByName("pvc", pvc.getMetadata().getName());
                    }
                    return false;
                }
            });
        LOGGER.info("PVC for cluster {} was deleted", clusterName);
    }

    public static void waitForPVCDeletion(int kafkaReplicas, JbodStorage jbodStorage, String clusterName) {
        TestUtils.waitFor("Wait for PVC deletion", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT, () -> {
            List<String> pvcs = kubeClient().listPersistentVolumeClaims().stream()
                    .map(pvc -> pvc.getMetadata().getName())
                    .collect(Collectors.toList());
            boolean isCorrect = false;

            for (SingleVolumeStorage singleVolumeStorage : jbodStorage.getVolumes()) {
                for (int i = 0; i < kafkaReplicas; i++) {
                    String jbodPVCName = "data-" + singleVolumeStorage.getId() + "-" + clusterName + "-kafka-" + i;
                    boolean deleteClaim = ((PersistentClaimStorage) singleVolumeStorage).isDeleteClaim();

                    if ((deleteClaim && !pvcs.contains(jbodPVCName)) || (!deleteClaim && pvcs.contains(jbodPVCName))) {
                        isCorrect = true;
                    } else {
                        isCorrect = false;
                        break;
                    }
                }
                if (!isCorrect)
                    break;
            }

            return isCorrect;
        });
    }
}
