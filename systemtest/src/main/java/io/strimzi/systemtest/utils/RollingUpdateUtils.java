/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class RollingUpdateUtils {
    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateUtils.class);

    /**
     * Method to check that all pods for expected StatefulSet were rolled
     * @param namespaceName Namespace name
     * @param selector
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return true when the pods for StatefulSet are recreated
     */
    public static boolean componentHasRolled(String namespaceName, LabelSelector selector, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));

        Map<String, String> currentSnapshot = PodUtils.podSnapshot(namespaceName, selector);

        LOGGER.debug("Current snapshot: {}", new TreeMap<>(currentSnapshot));
        // rolled when all the pods in snapshot have a different version in map

        currentSnapshot.keySet().retainAll(snapshot.keySet());

        LOGGER.debug("Pods in common: {}", new TreeMap<>(currentSnapshot));
        for (Map.Entry<String, String> podSnapshot : currentSnapshot.entrySet()) {
            String currentPodVersion = podSnapshot.getValue();
            String podName = podSnapshot.getKey();
            String oldPodVersion = snapshot.get(podName);
            if (oldPodVersion.equals(currentPodVersion)) {
                LOGGER.debug("At least {} hasn't rolled", podName);
                return false;
            }
        }

        LOGGER.debug("All pods seem to have rolled");
        return true;
    }

    public static boolean componentHasRolled(LabelSelector selector, Map<String, String> snapshot) {
        return componentHasRolled(kubeClient().getNamespace(), selector, snapshot);
    }

    /**
     *  Method to wait when StatefulSet will be recreated after rolling update
     * @param namespaceName Namespace name
     * @param selector
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return The snapshot of the StatefulSet after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillComponentHasRolled(String namespaceName, LabelSelector selector, Map<String, String> snapshot) {
        String componentName = selector.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL);
        LOGGER.info("Waiting for component with name: {} rolling update", componentName);
        TestUtils.waitFor("component with name " + componentName + " rolling update",
            Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(snapshot.size()), () -> {
                try {
                    return componentHasRolled(namespaceName, selector, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });

        LOGGER.info("Component with name: {} has been successfully rolled", componentName);
        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static Map<String, String> waitTillComponentHasRolledAndPodsReady(LabelSelector selector, int expectedPods, Map<String, String> snapshot) {
        return waitTillComponentHasRolledAndPodsReady(kubeClient().getNamespace(), selector, expectedPods, snapshot);
    }

    public static Map<String, String> waitTillComponentHasRolledAndPodsReady(String namespaceName, LabelSelector selector, int expectedPods, Map<String, String> snapshot) {
        String clusterName = selector.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String componentName = selector.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL);

        waitTillComponentHasRolled(namespaceName, selector, snapshot);

        LOGGER.info("Waiting for {} Pod(s) of {} to be ready", expectedPods, componentName);
        PodUtils.waitForPodsReady(namespaceName, selector, expectedPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get()));

        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static Map<String, String> waitTillComponentHasRolled(LabelSelector selector, int expectedPods, Map<String, String> snapshot) {
        return waitTillComponentHasRolled(kubeClient().getNamespace(), selector, expectedPods, snapshot);
    }

    public static Map<String, String> waitTillComponentHasRolled(String namespaceName, LabelSelector selector, int expectedPods, Map<String, String> snapshot) {
        waitTillComponentHasRolled(namespaceName, selector, snapshot);
        waitForComponentAndPodsReady(namespaceName, selector, expectedPods);

        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static void waitForComponentAndPodsReady(LabelSelector selector, int expectedPods) {
        waitForComponentAndPodsReady(kubeClient().getNamespace(), selector, expectedPods);
    }

    public static void waitForComponentAndPodsReady(String namespaceName, LabelSelector selector, int expectedPods) {
        String clusterName = selector.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String componentName = selector.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL);

        LOGGER.info("Waiting for {} Pod(s) of {} to be ready", expectedPods, componentName);
        PodUtils.waitForPodsReady(namespaceName, selector, expectedPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get()));

        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
        LOGGER.info("Kafka: {} is ready", clusterName);
    }

    public static void waitForNoRollingUpdate(String namespaceName, LabelSelector selector, Map<String, String> pods) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Waiting for stability of rolling update will be not triggered", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                if (!componentHasRolled(namespaceName, selector, pods)) {
                    LOGGER.info("{} pods didn't roll. Remaining seconds for stability: {}", pods.toString(),
                        Constants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == Constants.GLOBAL_RECONCILIATION_COUNT;
                } else {
                    throw new RuntimeException(pods.toString() + " pods are rolling!");
                }
            }
        );
    }
}
