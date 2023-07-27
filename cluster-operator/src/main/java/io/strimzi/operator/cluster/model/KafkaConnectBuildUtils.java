/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.openshift.api.model.Build;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;

/**
 * Utility methods for Kafka Connect Build
 */
public class KafkaConnectBuildUtils {
    /**
     * Checks if Pod already completed
     *
     * @param pod   Pod which should be checked for completion
     *
     * @param containerName Name of the Connect build container
     *
     * @return      True if the Pod is already complete, false otherwise
     */
    @SuppressWarnings("BooleanExpressionComplexity")
    public static boolean buildPodComplete(Pod pod, String containerName)   {
        return pod != null
                && pod.getStatus() != null
                && pod.getStatus().getContainerStatuses() != null
                && pod.getStatus().getContainerStatuses().size() > 0
                && getConnectBuildContainerStateTerminated(pod, containerName) != null;
    }

    /**
     * Checks if the pod completed with success
     *
     * @param pod   Pod which should be checked for completion
     *
     * @param containerName Name of the Connect build container
     *
     * @return      True if the Pod is complete with success, false otherwise
     */
    public static boolean buildPodSucceeded(Pod pod, String containerName)   {
        return buildPodComplete(pod, containerName)
                && getConnectBuildContainerStateTerminated(pod, containerName).getExitCode() == 0;
    }

    /**
     * Checks if the pod completed with error
     *
     * @param pod   Pod which should be checked for completion
     *
     * @param containerName Name of the Connect build container
     *
     * @return      True if the Pod is complete with error, false otherwise
     */
    public static boolean buildPodFailed(Pod pod, String containerName)   {
        return buildPodComplete(pod, containerName)
                && getConnectBuildContainerStateTerminated(pod, containerName).getExitCode() != 0;
    }

    /**
     * Checks if the build completed with error
     *
     * @param build Build which should be checked for completion
     *
     * @return      True if the Build is complete with error, false otherwise
     */
    public static boolean buildFailed(Build build)   {
        return build != null
                && build.getStatus() != null
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
        return build != null
                && build.getStatus() != null
                && "Complete".equals(build.getStatus().getPhase());
    }

    /**
     * Checks if the build completed with success
     *
     * @param build Build which should be checked for completion
     *
     * @return      True if the Build is complete with any result, false otherwise
     */
    public static boolean buildComplete(Build build)   {
        return buildSucceeded(build) || buildFailed(build);
    }

    /**
     * Returns the name of the Kafka Connect {@code Build} container.
     *
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @param isOpenshift Flag for determining, if we are running on Openshift
     *
     * @return The name of the corresponding Kafka Connect {@code Build} Container.
     */
    public static String getBuildContainerName(String clusterName, boolean isOpenshift) {
        return isOpenshift ? "docker-build" : KafkaConnectResources.buildPodName(clusterName);
    }

    /**
     * Get ContainerStateTerminated of the Connect build container
     *
     * @param pod Pod which contains ContainerStatus of the Connect build
     *
     * @param containerName Name of the Connect build container
     *
     * @return ContainerStateTerminated of the build container
     */
    public static ContainerStateTerminated getConnectBuildContainerStateTerminated(Pod pod, String containerName)   {
        ContainerStatus containerStatus = pod.getStatus().getContainerStatuses().stream().filter(conStatus -> conStatus.getName().equals(containerName))
            .findFirst().orElse(null);

        return containerStatus != null
                && containerStatus.getState() != null ? containerStatus.getState().getTerminated() : null;
    }
}
