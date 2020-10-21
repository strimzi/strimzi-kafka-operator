/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class PodUtils {

    private static final Logger LOGGER = LogManager.getLogger(PodUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(Constants.POD);

    private PodUtils() { }

    /**
     * Returns a map of resource name to resource version for all the pods in the given {@code namespace}
     * matching the given {@code selector}.
     */
    public static Map<String, String> podSnapshot(LabelSelector selector) {
        List<Pod> pods = kubeClient().listPods(selector);
        return pods.stream()
            .collect(
                Collectors.toMap(pod -> pod.getMetadata().getName(),
                    pod -> pod.getMetadata().getUid()));
    }

    public static String getFirstContainerImageNameFromPod(String podName) {
        return kubeClient().getPod(podName).getSpec().getContainers().get(0).getImage();
    }

    public static String getContainerImageNameFromPod(String podName, String containerName) {
        return kubeClient().getPod(podName).getSpec().getContainers().stream()
            .filter(c -> c.getName().equals(containerName))
            .findFirst().get().getImage();
    }

    public static String getInitContainerImageName(String podName) {
        return kubeClient().getPod(podName).getSpec().getInitContainers().get(0).getImage();
    }

    public static void waitForPodsReady(LabelSelector selector, int expectPods, boolean containers) {
        waitForPodsReady(selector, expectPods, containers, () -> { });
    }

    public static void waitForPodsReady(LabelSelector selector, int expectPods, boolean containers, Runnable onTimeout) {
        int[] counter = {0};

        TestUtils.waitFor("All pods matching " + selector + "to be ready",
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, ResourceOperation.timeoutForPodsOperation(expectPods),
            () -> {
                List<Pod> pods = kubeClient().listPods(selector);
                if (pods.isEmpty() && expectPods == 0) {
                    LOGGER.debug("Expected pods are ready");
                    return true;
                }
                if (pods.isEmpty()) {
                    LOGGER.debug("Not ready (no pods matching {})", selector);
                    return false;
                }
                if (pods.size() != expectPods) {
                    LOGGER.debug("Expected pods not ready");
                    return false;
                }
                for (Pod pod : pods) {
                    if (!Readiness.isPodReady(pod)) {
                        LOGGER.debug("Not ready (at least 1 pod not ready: {})", pod.getMetadata().getName());
                        counter[0] = 0;
                        return false;
                    } else {
                        if (containers) {
                            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                                LOGGER.debug("Not ready (at least 1 container of pod {} not ready: {})", pod.getMetadata().getName(), cs.getName());
                                if (!Boolean.TRUE.equals(cs.getReady())) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                LOGGER.debug("Pods {} are ready",
                    pods.stream().map(p -> p.getMetadata().getName()).collect(Collectors.joining(", ")));
                // When pod is up, it will check that are rolled pods are stable for next 10 polls and then it return true
                return ++counter[0] > 10;
            }, onTimeout);
    }

    public static void waitForPodUpdate(String podName, Date startTime) {
        TestUtils.waitFor(podName + " update", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT, () ->
            startTime.before(kubeClient().getCreationTimestampForPod(podName))
        );
    }

    /**
     * The method to wait when all pods of specific prefix will be deleted
     * To wait for the cluster to be updated, the following methods must be used: {@link StatefulSetUtils#ssHasRolled(String, Map)}, {@link StatefulSetUtils#waitTillSsHasRolled(String, int, Map)} )}
     * @param podsNamePrefix Cluster name where pods should be deleted
     */
    public static void waitForPodsWithPrefixDeletion(String podsNamePrefix) {
        LOGGER.info("Waiting when all Pods with prefix {} will be deleted", podsNamePrefix);
        kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(podsNamePrefix))
            .forEach(p -> deletePodWithWait(p.getMetadata().getName()));
    }

    public static String getPodNameByPrefix(String prefix) {
        return kubeClient().listPods().stream().filter(pod -> pod.getMetadata().getName().startsWith(prefix))
            .findFirst().get().getMetadata().getName();
    }

    public static String getFirstPodNameContaining(String searchTerm) {
        return kubeClient().listPods().stream().filter(pod -> pod.getMetadata().getName().contains(searchTerm))
                .findFirst().get().getMetadata().getName();
    }

    public static void waitForPod(String name) {
        LOGGER.info("Waiting when Pod {} will be ready", name);

        TestUtils.waitFor("pod " + name + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, ResourceOperation.timeoutForPodsOperation(1),
            () -> {
                List<ContainerStatus> statuses =  kubeClient().getPod(name).getStatus().getContainerStatuses();
                for (ContainerStatus containerStatus : statuses) {
                    if (!containerStatus.getReady()) {
                        return false;
                    }
                }
                return true;
            });
        LOGGER.info("Pod {} is ready", name);
    }

    public static void deletePodWithWait(String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("Pod " + name + " could not be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                List<Pod> pods = kubeClient().listPodsByPrefixInName(name);
                if (pods.size() != 0) {
                    for (Pod pod : pods) {
                        if (pod.getStatus().getPhase().equals("Terminating")) {
                            LOGGER.debug("Deleting pod {}", pod.getMetadata().getName());
                            cmdKubeClient().deleteByName("pod", pod.getMetadata().getName());
                        }
                    }
                    return false;
                } else {
                    return true;
                }
            });
        LOGGER.info("Pod {} deleted", name);
    }

    public static void waitUntilPodsCountIsPresent(String podNamePrefix, int numberOfPods) {
        LOGGER.info("Wait until {} Pods with prefix {} are present", numberOfPods, podNamePrefix);
        TestUtils.waitFor("", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPodsByPrefixInName(podNamePrefix).size() == numberOfPods);
        LOGGER.info("Pods with count {} are present", numberOfPods);
    }

    public static void waitUntilMessageIsInPodLogs(String podName, String message) {
        LOGGER.info("Waiting for message will be in the log");
        TestUtils.waitFor("Waiting for message will be in the log", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_LOG,
            () -> kubeClient().logs(podName).contains(message));
        LOGGER.info("Message {} found in {} log", message, podName);
    }

    public static void waitUntilMessageIsInLogs(String podName, String containerName, String message) {
        LOGGER.info("Waiting for message will be in the log");
        TestUtils.waitFor("Waiting for message will be in the log", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_LOG,
            () -> kubeClient().logs(podName, containerName).contains(message));
        LOGGER.info("Message {} found in {}:{} log", message, podName, containerName);
    }

    public static void waitUntilPodContainersCount(String podNamePrefix, int numberOfContainers) {
        LOGGER.info("Wait until Pod {} will have {} containers", podNamePrefix, numberOfContainers);
        TestUtils.waitFor("Pod " + podNamePrefix + " will have " + numberOfContainers + " containers",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPodsByPrefixInName(podNamePrefix).get(0).getSpec().getContainers().size() == numberOfContainers);
        LOGGER.info("Pod {} has {} containers", podNamePrefix, numberOfContainers);
    }

    public static void waitUntilPodStabilityReplicasCount(String podNamePrefix, int expectedPods) {
        LOGGER.info("Wait until Pod {} will have stable {} replicas", podNamePrefix, expectedPods);
        int[] stableCounter = {0};
        TestUtils.waitFor("Pod" + podNamePrefix + " will have " + expectedPods + " replicas",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                if (kubeClient().listPodsByPrefixInName(podNamePrefix).size() == expectedPods) {
                    stableCounter[0]++;
                    if (stableCounter[0] == Constants.GLOBAL_STABILITY_OFFSET_COUNT) {
                        LOGGER.info("Pod replicas are stable for {} polls intervals", stableCounter[0]);
                        return true;
                    }
                } else {
                    LOGGER.info("Pod replicas are not stable. Going to set the counter to zero.");
                    stableCounter[0] = 0;
                    return false;
                }
                LOGGER.info("Pod replicas gonna be stable in {} polls", Constants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
                return false;
            });
        LOGGER.info("Pod {} has {} replicas", podNamePrefix, expectedPods);
    }

    public static void waitUntilPodIsInCrashLoopBackOff(String podName) {
        LOGGER.info("Wait until Pod {} is in CrashLoopBackOff state", podName);
        TestUtils.waitFor("Pod {} is in CrashLoopBackOff state",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().getPod(podName).getStatus().getContainerStatuses().get(0)
                .getState().getWaiting().getReason().equals("CrashLoopBackOff"));
        LOGGER.info("Pod {} is in CrashLoopBackOff state", podName);
    }

    public static void waitUntilPodIsPresent(String podNamePrefix) {
        LOGGER.info("Wait until Pod {} is present", podNamePrefix);
        TestUtils.waitFor("Pod is present",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPodsByPrefixInName(podNamePrefix).get(0) != null);
        LOGGER.info("Pod {} is present", podNamePrefix);
    }

    public static void waitUntilPodIsInPendingStatus(String podName) {
        LOGGER.info("Wait until Pod {} is in pending state", podName);
        TestUtils.waitFor("Pod " + podName + " is in pending state", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->  kubeClient().getPod(podName).getStatus().getPhase().equals("Pending")
        );
        LOGGER.info("Pod:" + podName + " is in pending status");
    }

    public static void waitUntilPodLabelsDeletion(String podName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Pod label {} change to {}", labelKey, null);
            TestUtils.waitFor("Pod label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    kubeClient().getPod(podName).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("Pod label {} changed to {}", labelKey, null);
        }
    }

    /**
     * Ensures that at least one pod from listed (by prefix) is in {@code Pending} phase
     * @param podPrefix - all pods that matched the prefix will be verified
     */
    public static void waitForPendingPod(String podPrefix) {
        LOGGER.info("Wait for at least one pod with prefix: {} will be in pending phase", podPrefix);
        TestUtils.waitFor("One pod to be in PENDING state", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> actualPods = kubeClient().listPodsByPrefixInName(podPrefix);
                return actualPods.stream().anyMatch(pod -> pod.getStatus().getPhase().equals("Pending"));
            });
    }

    /**
     * Ensures every pod in a StatefulSet is stable in the {@code Running} phase.
     * A pod must be in the Running phase for {@link Constants#GLOBAL_RECONCILIATION_COUNT} seconds for
     * it to be considered as stable. Otherwise this procedure will be repeat.
     * @param podPrefix all pods that matched the prefix will be verified
     * */
    public static void verifyThatRunningPodsAreStable(String podPrefix) {
        LOGGER.info("Verify that all pods with prefix: {} are stable", podPrefix);
        verifyThatPodsAreStable(podPrefix);
    }

    private static void verifyThatPodsAreStable(String podPrefix) {
        int[] stabilityCounter = {0};
        List<Pod> runningPods = kubeClient().listPodsByPrefixInName(podPrefix).stream()
            .filter(pod -> pod.getStatus().getPhase().equals("Running")).collect(Collectors.toList());

        TestUtils.waitFor("Pods stability", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> actualPods = runningPods.stream().map(p -> kubeClient().getPod(p.getMetadata().getName())).collect(Collectors.toList());

                for (Pod pod : actualPods) {
                    if (pod.getStatus().getPhase().equals("Running")) {
                        LOGGER.info("Pod {} is in the {} state. Remaining seconds pod to be stable {}",
                            pod.getMetadata().getName(), pod.getStatus().getPhase(),
                            Constants.GLOBAL_RECONCILIATION_COUNT - stabilityCounter[0]);
                    } else {
                        LOGGER.info("Pod {} is not stable in phase following phase {} reset the stability counter from {} to {}",
                            pod.getMetadata().getName(), pod.getStatus().getPhase(), stabilityCounter[0], 0);
                        stabilityCounter[0] = 0;
                        return false;
                    }
                }
                stabilityCounter[0]++;

                if (stabilityCounter[0] == Constants.GLOBAL_RECONCILIATION_COUNT) {
                    LOGGER.info("All pods are stable {}", actualPods.stream().map(p -> p.getMetadata().getName()).collect(Collectors.joining(" ,")));
                    return true;
                }
                return false;
            });
    }
}
