/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

/**
 * In the future, as we better utilize the StrimziPodSet possibilities and not just replace StatefulSets 1-to-1, the
 * revision might require more complicated setup. That is why this is using separate class, although it currently seems
 * a bit simple.
 */
public class PodRevision {
    /**
     * Annotation for tracking pod revisions
     */
    public static final String STRIMZI_REVISION_ANNOTATION = Labels.STRIMZI_DOMAIN + "revision";
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(PodRevision.class.getName());

    /**
     * Generates the revision of the Pod. Currently, it just serializes the Pod using Jackson ObjectMapper and creates
     * a SHA1 hashstub from it.
     *
     * @param reconciliation    Reconciliation identifier (used for logging)
     * @param pod               Pod for which the revision should be calculated
     *
     * @return                  The revision string
     */
    public static String getRevision(Reconciliation reconciliation, Pod pod) {
        try {
            return Util.hashStub(PodSetUtils.podToString(pod));
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to get pod revision", e);
            throw new RuntimeException("Failed to get pod revision", e);
        }
    }

    /**
     * Compares the current Pod with the desired StrimziPodSet to decide if the desired pod changed and needs to be rolled.
     * It uses the revision to detect the changes. The desired revision is compared with the current revision.
     *
     * @param currentPod        Current pod
     * @param desiredPodSet    The StrimziPodSet resource with the desired Pods
     *
     * @return                  True if the revision changed. False otherwise.
     */
    public static boolean hasChanged(Pod currentPod, StrimziPodSet desiredPodSet)   {
        Pod desiredPod = desiredPodSet
                .getSpec()
                .getPods()
                .stream()
                .map(pod -> PodSetUtils.mapToPod(pod))
                .filter(pod -> currentPod.getMetadata().getName().equals(pod.getMetadata().getName()))
                .findFirst()
                .orElse(null);

        if (desiredPod != null) {
            return hasChanged(currentPod, desiredPod);
        } else {
            throw new RuntimeException("Pod " + currentPod.getMetadata().getName() + " in namespace " + currentPod.getMetadata().getNamespace() + " not found in desired StrimziPodSet");
        }
    }

    /**
     * Compares the current Pod with the desired Pod to decide if the desired pod changed and needs to be rolled.
     * It uses the revision to detect the changes. The desired revision is compared with the current revision.
     *
     * @param currentPod        Current pod
     * @param desiredPod        Desired pod
     *
     * @return                  True if the revision changed. False otherwise.
     */
    public static boolean hasChanged(Pod currentPod, Pod desiredPod)   {
        String currentRevision = getRevisionFromAnnotations(currentPod);
        String desiredRevision = getRevisionFromAnnotations(desiredPod);

        if (currentRevision == null && desiredRevision == null) {
            // Both revisions are null => that is weird, but it means they had not changed
            return false;
        } else if (currentRevision != null) {
            // Revisions differ => revision has changed
            return !currentRevision.equals(desiredRevision);
        } else {
            // currentRevision is null and desiredRevision is not null => revision has changed
            return true;
        }
    }

    /**
     * Helper method to extract the revision from the Pod metadata
     *
     * @param pod   Pod with the revision annotation
     *
     * @return      Current revision of the Pod
     */
    private static String getRevisionFromAnnotations(Pod pod)  {
        return Annotations.stringAnnotation(pod, STRIMZI_REVISION_ANNOTATION, null);
    }
}
