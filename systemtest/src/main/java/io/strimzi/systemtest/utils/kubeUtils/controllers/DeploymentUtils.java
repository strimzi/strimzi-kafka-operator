/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;

public class DeploymentUtils {

    private static final Logger LOGGER = LogManager.getLogger(DeploymentUtils.class);

    private DeploymentUtils() { }

    /**
     * Returns a map of pod name to resource version for the pods currently in the given deployment.
     * @param name The Deployment name.
     * @return A map of pod name to resource version for pods in the given Deployment.
     */
    public static Map<String, String> depSnapshot(String name) {
        Deployment deployment = kubeClient().getDeployment(name);
        LabelSelector selector = deployment.getSpec().getSelector();
        return PodUtils.podSnapshot(selector);
    }

    /**
     * Returns a map of pod name to resource version for the pods currently in the given DeploymentConfig.
     * @param name The DeploymentConfig name.
     * @return A map of pod name to resource version for pods in the given DeploymentConfig.
     */
    public static Map<String, String> depConfigSnapshot(String name) {
        LabelSelector selector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(name)).build();
        return PodUtils.podSnapshot(selector);
    }

    /**
     * Method to check that all pods for expected Deployment were rolled
     * @param name Deployment name
     * @param snapshot Snapshot of pods for Deployment before the rolling update
     * @return true when the pods for Deployment are recreated
     */
    public static boolean depHasRolled(String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        Map<String, String> map = PodUtils.podSnapshot(kubeClient().getDeployment(name).getSpec().getSelector());
        LOGGER.debug("Current  snapshot: {}", new TreeMap<>(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.debug("All pods seem to have rolled");
            return true;
        } else {
            LOGGER.debug("Some pods still to roll: {}", map);
            return false;
        }
    }

    /**
     * Method to check that all pods for expected DeploymentConfig were rolled
     * @param name DeploymentConfig name
     * @param snapshot Snapshot of pods for DeploymentConfig before the rolling update
     * @return true when the pods for DeploymentConfig are recreated
     */
    public static boolean depConfigHasRolled(String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        LabelSelector selector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(name)).build();
        Map<String, String> map = PodUtils.podSnapshot(selector);
        LOGGER.debug("Current  snapshot: {}", new TreeMap<>(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.info("All pods seem to have rolled");
            return true;
        } else {
            LOGGER.debug("Some pods still to roll: {}", map);
            return false;
        }
    }

    /**
     * Method to wait when Deployment will be recreated after rolling update
     * @param name Deployment name
     * @param expectedPods Expected number of pods
     * @param snapshot Snapshot of pods for Deployment before the rolling update
     * @return The snapshot of the Deployment after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillDepHasRolled(String name, int expectedPods, Map<String, String> snapshot) {
        LOGGER.info("Waiting for Deployment {} rolling update", name);
        TestUtils.waitFor("Deployment " + name + " rolling update",
            Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT, () -> depHasRolled(name, snapshot));
        waitForDeploymentReady(name);
        PodUtils.waitForPodsReady(kubeClient().getDeployment(name).getSpec().getSelector(), expectedPods, true);
        LOGGER.info("Deployment {} rolling update finished", name);
        return depSnapshot(name);
    }

    /**
     * Method to wait when DeploymentConfig will be recreated after rolling update
     * @param clusterName Kafka Connect S2I cluster name
     * @param snapshot Snapshot of pods for DeploymentConfig before the rolling update
     * @return The snapshot of the DeploymentConfig after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillDepConfigHasRolled(String clusterName, Map<String, String> snapshot) {
        LOGGER.info("Waiting for Kafka Connect S2I cluster {} rolling update", clusterName);
        String name = KafkaConnectS2IResources.deploymentName(clusterName);
        TestUtils.waitFor("DeploymentConfig roll of " + name,
            Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT, () -> depConfigHasRolled(name, snapshot));
        KafkaConnectS2IUtils.waitForConnectS2IStatus(clusterName, "Ready");
        return depConfigSnapshot(name);
    }

    public static void waitForPodUpdate(String podName, Date startTime) {
        TestUtils.waitFor(podName + " update", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS, () ->
            startTime.before(kubeClient().getCreationTimestampForPod(podName))
        );
    }

    /**
     * Wait until the given Deployment has been recovered.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentRecovery(String name, String deploymentUid) {
        LOGGER.info("Waiting for Deployment {}-{} recovery in namespace {}", name, deploymentUid, kubeClient().getNamespace());
        TestUtils.waitFor("deployment " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> !kubeClient().getDeploymentUid(name).equals(deploymentUid));
        LOGGER.info("Deployment {} was recovered", name);
    }

    /**
     * Wait until the given Deployment is ready.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentReady(String name) {
        LOGGER.debug("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getDeploymentStatus(name),
            () -> DeploymentUtils.logCurrentDeploymentStatus(kubeClient().getDeployment(name)));
        LOGGER.debug("Deployment {} is ready", name);
    }

    /**
     * Wait until the given Deployment is ready.
     * @param name The name of the Deployment.
     * @param expectPods The expected number of pods.
     */
    public static void waitForDeploymentReady(String name, int expectPods) {
        LOGGER.debug("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name + " pods to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getDeploymentStatus(name),
            () -> DeploymentUtils.logCurrentDeploymentStatus(kubeClient().getDeployment(name)));
        LOGGER.debug("Deployment {} is ready", name);
        LOGGER.debug("Waiting for Pods of Deployment {} to be ready", name);
        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(name), expectPods, true,
            () -> DeploymentUtils.logCurrentDeploymentStatus(kubeClient().getDeployment(name)));
    }

    /**
     * Wait until the given Deployment has been deleted.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentDeletion(String name) {
        LOGGER.debug("Waiting for Deployment {} deletion", name);
        TestUtils.waitFor("Deployment " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_RESOURCE_DELETION,
            () -> {
                if (kubeClient().getDeployment(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment {} is not deleted yet! Triggering force delete by cmd client!", name);
                    cmdKubeClient().deleteByName("deployment", name);
                    return false;
                }
            });
        LOGGER.debug("Deployment {} was deleted", name);
    }

    /**
     * Wait until the given DeploymentConfig has been deleted.
     * @param name The name of the DeploymentConfig.
     */
    public static void waitForDeploymentConfigDeletion(String name) {
        LOGGER.debug("Waiting for DeploymentConfig {} deletion", name);
        TestUtils.waitFor("DeploymentConfig " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, Constants.TIMEOUT_FOR_RESOURCE_DELETION,
            () -> {
                if (kubeClient().getDeploymentConfig(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment {} is not deleted yet! Triggering force delete by cmd client!", name);
                    cmdKubeClient().deleteByName("deploymentconfig", name);
                    return false;
                }
            });
        LOGGER.debug("Deployment {} was deleted", name);
    }

    public static void waitForNoRollingUpdate(String deploymentName, Map<String, String> pods) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Waiting for stability of rolling update will be not triggered", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                if (!DeploymentUtils.depHasRolled(deploymentName, pods)) {
                    LOGGER.info("{} pods not rolling waiting, remaining seconds for stability {}", pods.toString(),
                            Constants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == Constants.GLOBAL_RECONCILIATION_COUNT;
                } else {
                    throw new RuntimeException(pods.toString() + " pods are rolling!");
                }
            }
        );
    }

    /**
     * Wait until the given DeploymentConfig is ready.
     * @param name The name of the DeploymentConfig.
     */
    public static Map<String, String> waitForDeploymentConfigReady(String name, int expectPods) {
        LOGGER.debug("Waiting until DeploymentConfig {} is ready", name);
        TestUtils.waitFor("DeploymentConfig " + name + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getDeploymentConfigStatus(name));

        LOGGER.debug("DeploymentConfig {} is ready", name);
        LOGGER.debug("Waiting for Pods of DeploymentConfig {} to be ready", name);

        LabelSelector deploymentConfigSelector =
                new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(name)).build();
        PodUtils.waitForPodsReady(deploymentConfigSelector, expectPods, true);

        return depConfigSnapshot(name);
    }

    /**
     * Log actual status of deployment with pods
     * @param deployment - every DoneableDeployment, that HasMetadata and has status (fabric8 status)
     **/
    public static void logCurrentDeploymentStatus(Deployment deployment) {
        String kind = deployment.getKind();
        String name = deployment.getMetadata().getName();

        List<String> log = new ArrayList<>(asList("\n", kind, " status:\n", "\nConditions:\n"));

        for (DeploymentCondition deploymentCondition : deployment.getStatus().getConditions()) {
            log.add("\tType: " + deploymentCondition.getType() + "\n");
            log.add("\tMessage: " + deploymentCondition.getMessage() + "\n");
        }

        PodUtils.logCurrentPodStatus(kind, name, log);

        LOGGER.info("{}", String.join("", log));
    }
}
