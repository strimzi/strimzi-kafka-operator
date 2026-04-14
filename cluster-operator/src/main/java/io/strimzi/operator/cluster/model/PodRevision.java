/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.List;

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

    /**
     * Annotation for tracking pod resource revisions
     */
    public static final String STRIMZI_RESOURCE_REVISION_ANNOTATION = Labels.STRIMZI_DOMAIN + "resource-revision";

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(PodRevision.class.getName());

    private PodRevision() { }

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
            Pod podWithoutResources = new PodBuilder(pod).build();

            // Reset resources in any init containers if needed
            if (podWithoutResources.getSpec() != null && podWithoutResources.getSpec().getInitContainers() != null) {
                for (Container container : podWithoutResources.getSpec().getInitContainers()) {
                    container.setResources(null);
                }
            }

            // Reset resources in any regular containers if needed (should be never null, but we play it safe)
            if (podWithoutResources.getSpec() != null && podWithoutResources.getSpec().getContainers() != null) {
                for (Container container : podWithoutResources.getSpec().getContainers()) {
                    container.setResources(null);
                }
            }

            return Util.hashStub(PodSetUtils.podToString(podWithoutResources));
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to get pod revision", e);
            throw new RuntimeException("Failed to get pod revision", e);
        }
    }

    /**
     * Generates the revision of the Pod resources. Currently, it collects the different resource configurations,
     * serializes them using Jackson ObjectMapper and creates a SHA1 hashstub from it.
     *
     * @param reconciliation    Reconciliation identifier (used for logging)
     * @param pod               Pod for which the revision should be calculated
     *
     * @return                  The revision string
     */
    public static String getResourceRevision(Reconciliation reconciliation, Pod pod) {
        try {
            List<ResourceRequirements> resources = new ArrayList<>();

            // Reset resources in any init containers if needed
            if (pod.getSpec() != null && pod.getSpec().getInitContainers() != null) {
                for (Container container : pod.getSpec().getInitContainers()) {
                    if (container.getResources() != null) {
                        resources.add(container.getResources());
                    }
                }
            }

            // Reset resources in any regular containers if needed (should be never null, but we play it safe)
            if (pod.getSpec() != null && pod.getSpec().getContainers() != null) {
                for (Container container : pod.getSpec().getContainers()) {
                    if (container.getResources() != null) {
                        resources.add(container.getResources());
                    }
                }
            }

            return Util.hashStub(PodSetUtils.resourcesToString(resources));
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to get pod resource revision", e);
            throw new RuntimeException("Failed to get pod resource revision", e);
        }
    }

    /**
     * Compares the current Pod with the desired StrimziPodSet to decide if the desired pod changed and needs to be rolled.
     * It uses the revision to detect the changes. The desired revision is compared with the current revision.
     *
     * @param currentPod    Current pod
     * @param desiredPodSet The StrimziPodSet resource with the desired Pods
     * @param annotation    The annotation key to compare
     *
     * @return True if the revision changed. False otherwise.
     */
    public static boolean hasChanged(Pod currentPod, StrimziPodSet desiredPodSet, String annotation)   {
        return hasChanged(currentPod, PodSetUtils.findPodByName(currentPod.getMetadata().getName(), desiredPodSet), annotation);
    }

    /**
     * Compares the current Pod with the desired Pod to decide if the desired pod changed and needs to be rolled.
     * It uses the revision to detect the changes. The desired revision is compared with the current revision.
     *
     * @param currentPod    Current pod
     * @param desiredPod    Desired pod
     * @param annotation    The annotation key to compare
     *
     * @return True if the revision changed. False otherwise.
     */
    public static boolean hasChanged(Pod currentPod, Pod desiredPod, String annotation)   {
        String currentRevision = getRevisionFromAnnotations(currentPod, annotation);
        String desiredRevision = getRevisionFromAnnotations(desiredPod, annotation);

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
     * @param pod          Pod with the revision annotation
     * @param annotation   Annotation key to extract the revision from
     *
     * @return      Current revision of the Pod
     */
    private static String getRevisionFromAnnotations(Pod pod, String annotation)  {
        return Annotations.stringAnnotation(pod, annotation, null);
    }
}
