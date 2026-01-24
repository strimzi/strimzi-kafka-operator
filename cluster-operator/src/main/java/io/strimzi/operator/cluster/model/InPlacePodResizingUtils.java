/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

/**
 * Utilities used for in-place Pod resizing
 */
public class InPlacePodResizingUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(InPlacePodResizingUtils.class.getName());

    private InPlacePodResizingUtils() { }

    /**
     * Checks whether in-place resizing is enabled for the given resource
     *
     * @param resource  Resource to check the annotation on
     *
     * @return  True if in-place resizing is enabled. False otherwise.
     */
    public static boolean inPlaceResizingEnabled(HasMetadata resource)    {
        return Annotations.booleanAnnotation(resource, Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, false);
    }

    /**
     * Checks whether we should wait for deferred in-place resizing
     *
     * @param resource  Resource to check the annotation on
     *
     * @return  True if we should wait for deferred in-place resizing. False otherwise.
     */
    public static boolean inPlaceResizingWaitForDeferred(HasMetadata resource)    {
        return Annotations.booleanAnnotation(resource, Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING_WAIT_FOR_DEFERRED, false);
    }

    /**
     * Checks if the changes to resources are valid for in-place Pod resizing.
     *
     * @param currentPod    Current Pod
     * @param desiredPod    Desired Pod
     *
     * @return  True when the changes are valid. False otherwise.
     */
    public static boolean canResourcesBeUpdatedInPlace(Pod currentPod, Pod desiredPod) {
        for (Container container : currentPod.getSpec().getContainers()) {
            Container desired = desiredPod.getSpec().getContainers().stream().filter(c -> container.getName().equals(c.getName())).findFirst().orElse(null);
            if (desired != null) {
                if (!canResourcesBeUpdatedInPlace(container.getResources(), desired.getResources()))    {
                    return false;
                }
            } else {
                // A container from the current Pod is missing in the desired Pod. Let's avoid in-place resizing.
                // (This could be, for example, some custom container injected by some mutating webhook)
                return false;
            }
        }

        return true;
    }

    private static boolean canResourcesBeUpdatedInPlace(ResourceRequirements current, ResourceRequirements desired) {
        if (current == null && desired == null) {
            // Both are null => no resizing for this container, but other containers might be resizing
            return true;
        } else if (current == null) {
            // current is null, and desired is not null => we can try the in-place update
            return true;
        } else if (desired == null) {
            // desired is null, and current is not null => we cannot remove the resources
            return false;
        } else if ((current.getLimits() != null && desired.getLimits() == null)
                || (current.getLimits().get("cpu") != null && desired.getLimits().get("cpu") == null)
                || (current.getLimits().get("memory") != null && desired.getLimits().get("memory") == null)) {
            // Resource limits cannot be removed
            return false;
        }

        return true;
    }

    /**
     * Patches the container resources for dynamic Pod resizing.
     *
     * @param currentPod    Current Pod
     * @param desiredPod    Desired Pod
     *
     * @return  Pod with patched container resources
     */
    public static Pod patchPodResources(Pod currentPod, Pod desiredPod) {
        // We copy the pod instead of modifying the existing copy
        Pod patchedPod = new PodBuilder(currentPod).build();

        // Patch the resources for non-init containers
        for (Container container : patchedPod.getSpec().getContainers()) {
            Container desired = desiredPod.getSpec().getContainers().stream().filter(c -> container.getName().equals(c.getName())).findFirst().orElse(null);
            if (desired != null) {
                container.setResources(desired.getResources());
            } else {
                // This should never happen, but let's be safe
                throw new RuntimeException("Cannot patch pod resources: container " + container.getName() + " is missing in the desired Pod");
            }
        }

        return patchedPod;
    }

    /**
     * Checks if Pod needs to be restarted because dynamic Pod resizing failed.
     *
     * @param reconciliation    Reconciliation marker
     * @param pod               Pod to check for restart
     * @param waitForDeferred   Whether we should wait for deferred resizing or trigger a rolling update
     *
     * @return  Boolean if restart should be triggered. False otherwise.
     */
    /* test */ static boolean restartForResourceResizingNeeded(Reconciliation reconciliation, Pod pod, boolean waitForDeferred)  {
        if (pod.getStatus() != null
                && pod.getStatus().getConditions() != null
                && !pod.getStatus().getConditions().isEmpty()) {
            for (PodCondition condition : pod.getStatus().getConditions()) {
                if ("PodResizePending".equals(condition.getType())
                        && "True".equals(condition.getStatus())
                        && "Deferred".equals(condition.getReason())) {
                    if (waitForDeferred) {
                        LOGGER.warnCr(reconciliation, "Pod {} in namespace {} resizing has been deferred. Use manual rolling update if you want to roll the Pod.", pod.getMetadata().getName(), reconciliation.namespace());
                    } else {
                        LOGGER.infoCr(reconciliation, "Pod {} in namespace {} resizing has been deferred. Pod will be restarted.", pod.getMetadata().getName(), reconciliation.namespace());
                        return true;
                    }
                } else if ("PodResizePending".equals(condition.getType())
                        && "True".equals(condition.getStatus())
                        && "Infeasible".equals(condition.getReason())) {
                    LOGGER.infoCr(reconciliation, "Pod {} in namespace {} resizing is infeasible. Pod will be restarted.", pod.getMetadata().getName(), reconciliation.namespace());
                    return true;
                } else if ("PodResizeInProgress".equals(condition.getType())
                        && "True".equals(condition.getStatus())
                        && "Error".equals(condition.getReason())) {
                    LOGGER.infoCr(reconciliation, "Pod {} in namespace {} resizing failed because of '{}'. Pod will be restarted.", pod.getMetadata().getName(), reconciliation.namespace(), condition.getMessage());
                    return true;
                }
            }

            return false;
        } else {
            return false;
        }
    }

    /**
     * Checks if the Pod needs a rolling restart for any reasons related to resource resizing. This method is used in
     * Kafka, Kafka Connect and Kafka MirrorMaker2 rolling updates.
     *
     * @param reconciliation    Reconciliation marker
     * @param restartReasons    Restart reasons where we add any new reasons
     * @param podSet            PodSet with the desired Pod definition
     * @param pod               The current definition of the Pod
     */
    public static void reasonsToRestart(Reconciliation reconciliation, RestartReasons restartReasons, StrimziPodSet podSet, Pod pod) {
        if (InPlacePodResizingUtils.inPlaceResizingEnabled(podSet))   {
            // In-place resizing is enabled -> we still might need to restart it in some cases

            if (PodRevision.hasChanged(pod, podSet, PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION)
                    && !InPlacePodResizingUtils.canResourcesBeUpdatedInPlace(pod, PodSetUtils.findPodByName(pod.getMetadata().getName(), podSet)))  {
                // The resources changed and the change is not valid for in-place update
                restartReasons.add(RestartReason.POD_HAS_OLD_RESOURCE_REVISION);
            }

            if (PodRevision.hasChanged(pod, podSet, PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION)
                    && (pod.getStatus() == null || !"Running".equals(pod.getStatus().getPhase())))  {
                // The resources changed but the Pod is not in a Running state (could be stuck) -> needs to be rolled
                restartReasons.add(RestartReason.POD_HAS_OLD_RESOURCE_REVISION);
            }
        } else if (PodRevision.hasChanged(pod, podSet, PodRevision.STRIMZI_RESOURCE_REVISION_ANNOTATION))    {
            // In-place resizing is disabled, and resource revision changed -> we have a restart reason
            restartReasons.add(RestartReason.POD_HAS_OLD_RESOURCE_REVISION);
        }

        // We check this regardless whether the in-place resizing is enabled or not as it might have been enabled in the
        // past and the in-place resizing already failed. But normally, this restart reason should not happen when
        // in-place resizing is not used.
        if (InPlacePodResizingUtils.restartForResourceResizingNeeded(reconciliation, pod, InPlacePodResizingUtils.inPlaceResizingWaitForDeferred(podSet))) {
            restartReasons.add(RestartReason.POD_RESOURCES_CHANGED);
        }
    }
}
