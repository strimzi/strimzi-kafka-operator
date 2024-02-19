/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

public class RollingUpdateUtils {
    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateUtils.class);

    /**
     * Method to check that all Pods for expected component (StrimziPodSet, Deployment) were rolled
     * @param namespaceName Namespace name
     * @param selector
     * @param snapshot Snapshot of Pods for component (StrimziPodSet, Deployment) before the rolling update
     * @return true when the Pods for component (StrimziPodSet, Deployment) are recreated
     */
    public static boolean componentHasRolled(String namespaceName, LabelSelector selector, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}/{}", namespaceName, new TreeMap<>(snapshot));

        Map<String, String> currentSnapshot = PodUtils.podSnapshot(namespaceName, selector);

        LOGGER.debug("Current snapshot: {}/{}", namespaceName, new TreeMap<>(currentSnapshot));
        // rolled when all the Pods in snapshot have a different version in map

        currentSnapshot.keySet().retainAll(snapshot.keySet());

        LOGGER.debug("Pods in common: {}/{}", namespaceName, new TreeMap<>(currentSnapshot));
        for (Map.Entry<String, String> podSnapshot : currentSnapshot.entrySet()) {
            String currentPodVersion = podSnapshot.getValue();
            String podName = podSnapshot.getKey();
            String oldPodVersion = snapshot.get(podName);
            if (oldPodVersion.equals(currentPodVersion)) {
                LOGGER.debug("At least {}/{} hasn't rolled", namespaceName, podName);
                return false;
            }
        }

        LOGGER.debug("All Pods seem to have rolled");
        return true;
    }

    /**
     *  Method to wait when component (StrimziPodSet, Deployment) will be recreated after rolling update
     * @param namespaceName Namespace name
     * @param selector
     * @param snapshot Snapshot of Pods for  component (StrimziPodSet, Deployment) before the rolling update
     * @return The snapshot of the  component (StrimziPodSet, Deployment) after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillComponentHasRolled(String namespaceName, LabelSelector selector, Map<String, String> snapshot) {
        String componentName = getComponentNameFromLabelSelector(selector);

        LOGGER.info("Waiting for component matching {} -> {}/{} rolling update", selector, namespaceName, componentName);
        TestUtils.waitFor("rolling update of component: " + namespaceName + "/" + componentName,
            TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(snapshot.size()), () -> {
                try {
                    return componentHasRolled(namespaceName, selector, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });

        LOGGER.info("Component matching {} -> {}/{} has been successfully rolled", selector, namespaceName, componentName);
        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static Map<String, String> waitTillComponentHasRolledAndPodsReady(String namespaceName, LabelSelector selector, int expectedPods, Map<String, String> snapshot) {
        String clusterName = selector.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String componentName = getComponentNameFromLabelSelector(selector);

        waitTillComponentHasRolled(namespaceName, selector, snapshot);

        LOGGER.info("Waiting for {} Pod(s) of {}/{} to be ready", expectedPods, namespaceName, componentName);
        PodUtils.waitForPodsReady(namespaceName, selector, expectedPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get()));

        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static Map<String, String> waitTillComponentHasRolled(String namespaceName, LabelSelector selector, int expectedPods, Map<String, String> snapshot) {
        waitTillComponentHasRolled(namespaceName, selector, snapshot);
        waitForComponentAndPodsReady(namespaceName, selector, expectedPods);

        return PodUtils.podSnapshot(namespaceName, selector);
    }

    /**
     *  Method to wait when Kafka or Zookeeper starts rolling update by rolling first Pod
     * @param namespaceName Namespace name
     * @param selector
     * @param snapshot Snapshot of Kafka or Zookeeper Pods before the rolling update
     * @return The new Snapshot of actually present Pods after the first successful roll
     */
    public static Map<String, String> waitTillComponentHasStartedRolling(String namespaceName, LabelSelector selector, Map<String, String> snapshot) {
        String clusterName = selector.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String componentName = selector.getMatchLabels().get(Labels.STRIMZI_CONTROLLER_NAME_LABEL);

        componentName = componentName == null ? clusterName + "-" + selector.getMatchLabels().get(Labels.STRIMZI_POOL_NAME_LABEL) : componentName;

        LOGGER.info("Waiting for component matching {} -> {}/{} first rolled Pod", selector, namespaceName, componentName);
        TestUtils.waitFor("first pod's roll : " + namespaceName + "/" + componentName,
            TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(snapshot.size()), () -> {
                try {
                    LOGGER.debug("Existing snapshot: {}/{}", namespaceName, new TreeMap<>(snapshot));

                    Map<String, String> currentSnapshot = PodUtils.podSnapshot(namespaceName, selector);
                    LOGGER.debug("Current snapshot: {}/{}", namespaceName, new TreeMap<>(currentSnapshot));

                    currentSnapshot.keySet().retainAll(snapshot.keySet());

                    LOGGER.debug("Pods in common: {}/{}", namespaceName, new TreeMap<>(currentSnapshot));
                    for (Map.Entry<String, String> podSnapshot : currentSnapshot.entrySet()) {
                        String currentPodVersion = podSnapshot.getValue();
                        String podName = podSnapshot.getKey();
                        String oldPodVersion = snapshot.get(podName);
                        if (!oldPodVersion.equals(currentPodVersion)) {
                            LOGGER.debug("Pod {}/{} rolled", namespaceName, podName);
                            return true;
                        }
                    }

                    LOGGER.debug("Component did not start Rolling Update");
                    return false;

                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        );

        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static void waitForComponentAndPodsReady(String namespaceName, LabelSelector selector, int expectedPods) {
        final String clusterName = selector.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String componentName = getComponentNameFromLabelSelector(selector);

        LOGGER.info("Waiting for {} Pod(s) of {}/{} to be ready", expectedPods, namespaceName, componentName);

        final Runnable componentLogAfterTimeout;
        final BooleanSupplier componentReadinessStatus;

        if (selector.getMatchLabels() != null && selector.getMatchLabels().containsKey(Labels.STRIMZI_KIND_LABEL)) {
            if (selector.getMatchLabels().get(Labels.STRIMZI_KIND_LABEL).equals(KafkaConnect.RESOURCE_KIND)) {
                componentLogAfterTimeout = () -> ResourceManager.logCurrentResourceStatus(KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get());
                componentReadinessStatus = () -> KafkaConnectUtils.waitForConnectReady(namespaceName, clusterName);
            } else if (selector.getMatchLabels().get(Labels.STRIMZI_KIND_LABEL).equals(KafkaMirrorMaker2.RESOURCE_KIND)) {
                componentLogAfterTimeout = () -> ResourceManager.logCurrentResourceStatus(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get());
                componentReadinessStatus = () -> KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(namespaceName, clusterName);
            } else if (selector.getMatchLabels().get(Labels.STRIMZI_KIND_LABEL).equals(Kafka.RESOURCE_KIND)) {
                componentLogAfterTimeout = () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get());
                componentReadinessStatus = () -> KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
            } else {
                throw new RuntimeException("Waiting for such component (" + selector.getMatchLabels().get(Labels.STRIMZI_KIND_LABEL) + ")  is not supported.");
            }
        } else {
            throw new RuntimeException("Selector does not contain " + Labels.STRIMZI_KIND_LABEL + " label.");
        }

        // 1. wait for readiness Pods
        PodUtils.waitForPodsReady(namespaceName, selector, expectedPods, true, componentLogAfterTimeout);

        // 2. wait for readiness of the status
        StUtils.waitUntilSupplierIsSatisfied(componentReadinessStatus);
    }

    public static void waitForNoRollingUpdate(String namespaceName, LabelSelector selector, Map<String, String> pods) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Pods to remain stable and rolling update not to be triggered", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                if (!componentHasRolled(namespaceName, selector, pods)) {
                    LOGGER.info("Pods {}/{} did not roll. Must remain stable for: {} second(s)", namespaceName, pods.toString(),
                        TestConstants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == TestConstants.GLOBAL_RECONCILIATION_COUNT;
                } else {
                    throw new RuntimeException(pods.toString() + " Pods are rolling!");
                }
            }
        );
    }

    public static Map<String, String> waitForComponentScaleUpOrDown(String namespaceName, LabelSelector selector, int expectedPods) {
        waitForComponentAndPodsReady(namespaceName, selector, expectedPods);
        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static void waitForNoKafkaAndZKRollingUpdate(String namespaceName, String clusterName, Map<String, String> brokerPods) {
        int[] i = {0};

        LabelSelector brokerSelector = KafkaResource.getLabelSelector(clusterName, StrimziPodSetResource.getBrokerComponentName(clusterName));

        TestUtils.waitFor("Kafka Pods to remain stable and rolling update not to be triggered", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                boolean kafkaRolled = componentHasRolled(namespaceName, brokerSelector, brokerPods);

                if (!kafkaRolled) {
                    LOGGER.info("Kafka Pods did not roll. Must remain stable for: {} second(s)", TestConstants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                } else {
                    throw new RuntimeException(brokerPods.toString() + " Pods are rolling!");
                }

                return i[0]++ == TestConstants.GLOBAL_RECONCILIATION_COUNT;
            }
        );
    }

    private static String getComponentNameFromLabelSelector(LabelSelector labelSelector) {
        String componentName;

        if (labelSelector.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL) != null) {
            componentName = labelSelector.getMatchLabels().get(Labels.STRIMZI_NAME_LABEL);
        } else if (labelSelector.getMatchLabels().get(Labels.STRIMZI_CONTROLLER_NAME_LABEL) != null) {
            componentName = labelSelector.getMatchLabels().get(Labels.STRIMZI_CONTROLLER_NAME_LABEL);
        } else {
            componentName = labelSelector.getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL) + "-" + labelSelector.getMatchLabels().get(Labels.STRIMZI_POOL_NAME_LABEL);
        }

        return componentName;
    }
}
