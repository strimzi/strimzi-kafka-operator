/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.strimzi.systemtest.Constants;
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

    private PersistentVolumeClaimUtils() { }

    public static void waitUntilPVCLabelsChange(Map<String, String> newLabels, String labelKey) {
        LOGGER.info("Waiting till PVC labels will change {}", newLabels.toString());
        TestUtils.waitFor("Waiting till PVC labels will change {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
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
        LOGGER.info("Waiting till PVC annotation will change {}", newAnnotation.toString());
        TestUtils.waitFor("Waiting till PVC labels will change {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
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
        LOGGER.info("Waiting till PVC deletion for cluster {}", clusterName);
        TestUtils.waitFor("Waiting till PVC will be deleted {}", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
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
}
