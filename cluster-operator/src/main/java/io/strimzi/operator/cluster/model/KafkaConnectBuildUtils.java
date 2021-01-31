/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.openshift.api.model.Build;

public class KafkaConnectBuildUtils {
    /**
     * Checks if Pod already completed
     *
     * @param pod   Pod which should be checked for completion
     *
     * @return      True if the Pod is already complete, false otherwise
     */
    public static boolean buildPodComplete(Pod pod)   {
        return pod.getStatus() != null
                && pod.getStatus().getContainerStatuses() != null
                && pod.getStatus().getContainerStatuses().size() > 0
                && pod.getStatus().getContainerStatuses().get(0) != null
                && pod.getStatus().getContainerStatuses().get(0).getState() != null
                && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null;
    }

    /**
     * Checks if the pod completed with success
     *
     * @param pod   Pod which should be checked for completion
     *
     * @return      True if the Pod is complete with success, false otherwise
     */
    public static boolean buildPodSucceeded(Pod pod)   {
        return buildPodComplete(pod)
                && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated().getExitCode() == 0;
    }

    /**
     * Checks if the pod completed with error
     *
     * @param pod   Pod which should be checked for completion
     *
     * @return      True if the Pod is complete with error, false otherwise
     */
    public static boolean buildPodFailed(Pod pod)   {
        return buildPodComplete(pod)
                && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated().getExitCode() != 0;
    }

    /**
     * Checks if the build completed with error
     *
     * @param build Build which should be checked for completion
     *
     * @return      True if the Build is complete with error, false otherwise
     */
    public static boolean buildFailed(Build build)   {
        return build.getStatus() != null
                && ("Failed".equals(build.getStatus().getPhase()) || "Error".equals(build.getStatus().getPhase()) || "Cancelled".equals(build.getStatus().getPhase()));
    }

    /**
     * Checks if the build completed with success
     *
     * @param build Build which should be checked for completion
     *
     * @return      True if the Build is complete with success, false otherwise
     */
    public static boolean buildSucceeded(Build build)   {
        return build.getStatus() != null && "Complete".equals(build.getStatus().getPhase());
    }

    /**
     * Checks if the build completed with wuccess
     *
     * @param build Build which should be checked for completion
     *
     * @return      True if the Build is complete with any result, false otherwise
     */
    public static boolean buildComplete(Build build)   {
        return buildSucceeded(build) || buildFailed(build);
    }
}
