/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class PodUtils {

    private static final Logger LOGGER = LogManager.getLogger(PodUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(Constants.POD);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.POD);

    private PodUtils() { }

    /**
     * Returns a map of resource name to resource version for all the pods in the given {@code namespace}
     * matching the given {@code selector}.
     */
    public static Map<String, String> podSnapshot(String namespaceName, LabelSelector selector) {
        List<Pod> pods = kubeClient(namespaceName).listPods(namespaceName, selector);
        return pods.stream()
            .collect(
                Collectors.toMap(pod -> pod.getMetadata().getName(),
                    pod -> pod.getMetadata().getUid()));
    }

    public static String getFirstContainerImageNameFromPod(String namespaceName, String podName) {
        return kubeClient(namespaceName).getPod(namespaceName, podName).getSpec().getContainers().get(0).getImage();
    }

    public static String getContainerImageNameFromPod(String namespaceName, String podName, String containerName) {
        return kubeClient(namespaceName).getPod(podName).getSpec().getContainers().stream()
            .filter(c -> c.getName().equals(containerName))
            .findFirst().orElseThrow().getImage();
    }
    
    public static String getInitContainerImageName(String podName) {
        return kubeClient().getPod(podName).getSpec().getInitContainers().get(0).getImage();
    }

    public static void waitForPodsReady(String namespaceName, LabelSelector selector, int expectPods, boolean containers) {
        waitForPodsReady(namespaceName, selector, expectPods, containers, () -> { });
    }

    public static void waitForPodsReady(String namespaceName, LabelSelector selector, int expectPods, boolean containers, Runnable onTimeout) {
        TestUtils.waitFor("All pods matching " + selector + "to be ready",
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, ResourceOperation.timeoutForPodsOperation(expectPods),
            () -> {
                List<Pod> pods = kubeClient(namespaceName).listPods(namespaceName, selector);
                if (pods.isEmpty() && expectPods == 0) {
                    LOGGER.debug("Expected pods are ready");
                    return true;
                }
                if (pods.isEmpty()) {
                    LOGGER.debug("Not ready (no pods matching {}/{})", namespaceName, selector);
                    return false;
                }
                if (pods.size() != expectPods) {
                    LOGGER.debug("Expected pods {}/{} are not ready", namespaceName, selector);
                    return false;
                }
                for (Pod pod : pods) {
                    if (!Readiness.isPodReady(pod)) {
                        LOGGER.debug("Not ready (at least 1 pod not ready: {}/{})", namespaceName, pod.getMetadata().getName());
                        return false;
                    } else {
                        if (containers) {
                            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                                if (!Boolean.TRUE.equals(cs.getReady())) {
                                    LOGGER.debug("Not ready (at least 1 container of pod {}/{} not ready: {})", namespaceName, pod.getMetadata().getName(), cs.getName());
                                    return false;
                                }
                            }
                        }
                    }
                }
                return true;
            }, onTimeout);
    }

    /**
     * The method to wait when all pods of specific prefix will be deleted
     * To wait for the cluster to be updated, the following methods must be used:
     * {@link io.strimzi.systemtest.utils.RollingUpdateUtils#componentHasRolled(String, LabelSelector, Map)},
     * {@link io.strimzi.systemtest.utils.RollingUpdateUtils#waitTillComponentHasRolled(String, LabelSelector, int, Map)} )}
     * @param podsNamePrefix Cluster name where pods should be deleted
     */
    public static void waitForPodsWithPrefixDeletion(String podsNamePrefix) {
        LOGGER.info("Waiting when all Pods with prefix {} will be deleted", podsNamePrefix);
        kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(podsNamePrefix))
            .forEach(p -> deletePodWithWait(p.getMetadata().getNamespace(), p.getMetadata().getName()));
    }

    public static String getPodNameByPrefix(String namespaceName, String prefix) {
        return kubeClient(namespaceName).listPods(namespaceName).stream().filter(pod -> pod.getMetadata().getName().startsWith(prefix))
            .findFirst().orElseThrow().getMetadata().getName();
    }

    public static List<Pod> getPodsByPrefixInNameWithDynamicWait(String namespaceName, String podNamePrefix) {
        AtomicReference<List<Pod>> result = new AtomicReference<>();

        TestUtils.waitFor("pod with prefix" + podNamePrefix + " is present.", Constants.GLOBAL_POLL_INTERVAL_MEDIUM, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> listOfPods = kubeClient(namespaceName).listPods(namespaceName)
                    .stream().filter(p -> p.getMetadata().getName().startsWith(podNamePrefix)
                        && !p.getMetadata().getName().contains("producer")
                        && !p.getMetadata().getName().contains("consumer")
                    ).collect(Collectors.toList());
                // true if number of pods is more than 1
                result.set(listOfPods);
                return result.get().size() > 0;
            });

        return result.get();
    }

    public static String getFirstPodNameContaining(final String namespaceName, String searchTerm) {
        return kubeClient(namespaceName).listPods(namespaceName).stream().filter(pod -> pod.getMetadata().getName().contains(searchTerm))
                .findFirst().orElseThrow().getMetadata().getName();
    }

    public static void deletePodWithWait(String namespaceName, String name) {
        LOGGER.info("Waiting when Pod {} will be deleted", name);

        TestUtils.waitFor("Pod " + name + " could not be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                List<Pod> pods = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, name);
                if (pods.size() != 0) {
                    for (Pod pod : pods) {
                        if (pod.getStatus().getPhase().equals("Terminating")) {
                            LOGGER.debug("Deleting pod {}", pod.getMetadata().getName());
                            cmdKubeClient(namespaceName).deleteByName("pod", pod.getMetadata().getName());
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

    public static void waitUntilMessageIsInPodLogs(final String namespaceName, String podName, String message, long duration) {
        LOGGER.info("Waiting for message will be in the log");
        TestUtils.waitFor("Waiting for message will be in the log", Constants.GLOBAL_POLL_INTERVAL, duration,
            () -> kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, podName).contains(message));
        LOGGER.info("Message {} found in {} log", message, podName);
    }

    public static void waitUntilMessageIsInPodLogs(final String namespaceName, String podName, String message) {
        waitUntilMessageIsInPodLogs(namespaceName, podName, message, Constants.TIMEOUT_FOR_LOG);
    }

    public static void waitUntilPodContainersCount(String namespaceName, String podNamePrefix, int numberOfContainers) {
        LOGGER.info("Wait until Pod {}/{} will have {} containers", namespaceName, podNamePrefix, numberOfContainers);
        TestUtils.waitFor("Pod " + podNamePrefix + " will have " + numberOfContainers + " containers",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, podNamePrefix).get(0).getSpec().getContainers().size() == numberOfContainers);
        LOGGER.info("Pod {}/{} has {} containers", namespaceName, podNamePrefix, numberOfContainers);
    }

    public static void waitUntilPodStabilityReplicasCount(String namespaceName, String podNamePrefix, int expectedPods) {
        LOGGER.info("Wait until Pod {}/{} will have stable {} replicas", namespaceName, podNamePrefix, expectedPods);
        int[] stableCounter = {0};
        TestUtils.waitFor(" Pod" + namespaceName + "/" + podNamePrefix + " will have " + expectedPods + " replicas",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                if (kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, podNamePrefix).size() == expectedPods) {
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
        LOGGER.info("Pod {}/{} has {} replicas", namespaceName, podNamePrefix, expectedPods);
    }

    public static void waitUntilPodIsInCrashLoopBackOff(String namespaceName, String podName) {
        LOGGER.info("Wait until Pod {}/{} is in CrashLoopBackOff state", namespaceName, podName);
        TestUtils.waitFor("Pod " + namespaceName + "/" + podName + " is in CrashLoopBackOff state",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(namespaceName).getPod(namespaceName, podName).getStatus().getContainerStatuses().get(0)
                .getState().getWaiting().getReason().equals("CrashLoopBackOff"));
        LOGGER.info("Pod {}/{} is in CrashLoopBackOff state", namespaceName, podName);
    }

    public static void waitUntilPodIsPresent(String namespaceName, String podNamePrefix) {
        LOGGER.info("Wait until Pod {}/{} is present", namespaceName, podNamePrefix);
        TestUtils.waitFor("Pod is present",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, podNamePrefix).get(0) != null);
        LOGGER.info("Pod {}/{} is present", namespaceName, podNamePrefix);
    }

    public static void waitUntilPodLabelsDeletion(String namespaceName, String podName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for Pod {}/{} label {} change to {}", namespaceName, podName, labelKey, null);
            TestUtils.waitFor("Pod label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    kubeClient(namespaceName).getPod(namespaceName, podName).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("Pod {}/{} label {} changed to {}", namespaceName, podName, labelKey, null);
        }
    }

    /**
     * Ensures that at least one pod from listed (by prefix) is in {@code Pending} phase
     * @param namespaceName Namespace name
     * @param podPrefix - all pods that matched the prefix will be verified
     */
    public static void waitForPendingPod(String namespaceName, String podPrefix) {
        LOGGER.info("Wait for at least one pod with prefix {}/{} will be in pending phase", namespaceName, podPrefix);
        TestUtils.waitFor("One pod to be in PENDING state", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> actualPods = kubeClient(namespaceName).listPodsByPrefixInName(podPrefix);
                return actualPods.stream().anyMatch(pod -> pod.getStatus().getPhase().equals("Pending"));
            });
    }

    /**
     * Ensures every pod in a StatefulSet is stable in the {@code Running} phase.
     * A pod must be in the Running phase for {@link Constants#GLOBAL_RECONCILIATION_COUNT} seconds for
     * it to be considered as stable. Otherwise this procedure will be repeat.
     * @param podPrefix all pods that matched the prefix will be verified
     * */
    public static void verifyThatRunningPodsAreStable(String namespaceName, String podPrefix) {
        LOGGER.info("Verify that all pods with prefix {}/{} are stable", namespaceName, podPrefix);
        verifyThatPodsAreStable(namespaceName, podPrefix, "Running");
    }

    public static void verifyThatPendingPodsAreStable(String namespaceName, String podPrefix) {
        LOGGER.info("Verify that all pods with prefix {}/{} are stable in pending phase", namespaceName, podPrefix);
        verifyThatPodsAreStable(namespaceName, podPrefix, "Pending");
    }

    private static void verifyThatPodsAreStable(String namespaceName, String podPrefix, String phase) {
        int[] stabilityCounter = {0};

        List<Pod> runningPods = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, podPrefix).stream()
            .filter(pod -> pod.getStatus().getPhase().equals(phase)).collect(Collectors.toList());

        TestUtils.waitFor(String.format("Pods %s#%s stability in phase %s", namespaceName, podPrefix, phase), Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> actualPods = runningPods.stream().map(p -> kubeClient(namespaceName).getPod(namespaceName, p.getMetadata().getName())).collect(Collectors.toList());

                for (Pod pod : actualPods) {
                    if (pod.getStatus().getPhase().equals(phase)) {
                        LOGGER.info("Pod {}/{} is in the {} state. Remaining seconds pod to be stable {}",
                                namespaceName, pod.getMetadata().getName(), pod.getStatus().getPhase(),
                            Constants.GLOBAL_RECONCILIATION_COUNT - stabilityCounter[0]);
                    } else {
                        LOGGER.info("Pod {}/{} is not stable in phase following phase {} reset the stability counter from {} to {}",
                                namespaceName, pod.getMetadata().getName(), pod.getStatus().getPhase(), stabilityCounter[0], 0);
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

    public static void waitForPodContainerReady(String namespaceName, String podName, String containerName) {
        LOGGER.info("Waiting for Pod {}/{} container {} will be ready", namespaceName, podName, containerName);
        TestUtils.waitFor("Pod " + namespaceName + "/" + podName + " container " + containerName + "will be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT, () ->
            kubeClient(namespaceName).getPod(podName).getStatus().getContainerStatuses().stream().filter(container -> container.getName().equals(containerName)).findFirst().orElseThrow().getReady()
        );
        LOGGER.info("Pod {}/{} container {} is ready", namespaceName, podName, containerName);
    }

    /**
     * Retrieves a list of Kafka cluster Pods based on TestStorage cluster name attribute {@code testStorage.getClusterName()}.
     *
     * @param testStorage   TestStorage of specific test case
     * @return              Returns a list of Kafka cluster Pods (i.e.., Kafka, ZooKeeper, EO).
     */
    public static List<Pod> getKafkaClusterPods(final TestStorage testStorage) {
        List<Pod> kafkaClusterPods = kubeClient(testStorage.getNamespaceName())
            .listPodsByPrefixInName(testStorage.getKafkaStatefulSetName());
        // zk pods
        kafkaClusterPods.addAll(kubeClient(testStorage.getNamespaceName())
            .listPodsByPrefixInName(testStorage.getZookeeperStatefulSetName()));
        // eo pod
        kafkaClusterPods.addAll(kubeClient(testStorage.getNamespaceName())
            .listPodsByPrefixInName(testStorage.getEoDeploymentName()));

        return kafkaClusterPods;
    }
}
