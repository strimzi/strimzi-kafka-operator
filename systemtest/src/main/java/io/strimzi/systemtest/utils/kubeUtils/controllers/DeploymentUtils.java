/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;

public class DeploymentUtils {

    private static final Logger LOGGER = LogManager.getLogger(DeploymentUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.DEPLOYMENT);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private DeploymentUtils() { }

    /**
     * Log actual status of deployment with pods
     * @param deployment - every Deployment, that HasMetadata and has status (fabric8 status)
     **/
    public static void logCurrentDeploymentStatus(Deployment deployment) {
        if (deployment != null) {
            String kind = deployment.getKind();
            String name = deployment.getMetadata().getName();

            List<String> log = new ArrayList<>(asList("\n", kind, " status:\n", "\nConditions:\n"));

            for (DeploymentCondition deploymentCondition : deployment.getStatus().getConditions()) {
                log.add("\tType: " + deploymentCondition.getType() + "\n");
                log.add("\tMessage: " + deploymentCondition.getMessage() + "\n");
            }

            if (kubeClient().listPodsByPrefixInName(name).size() != 0) {
                log.add("\nPods with conditions and messages:\n\n");

                for (Pod pod : kubeClient().listPodsByPrefixInName(name)) {
                    log.add(pod.getMetadata().getName() + ":");
                    for (PodCondition podCondition : pod.getStatus().getConditions()) {
                        if (podCondition.getMessage() != null) {
                            log.add("\n\tType: " + podCondition.getType() + "\n");
                            log.add("\tMessage: " + podCondition.getMessage() + "\n");
                        }
                    }
                    log.add("\n\n");
                }
                LOGGER.info("{}", String.join("", log));
            }

            LOGGER.info("{}", String.join("", log));
        }
    }

    public static void waitForNoRollingUpdate(String deploymentName, Map<String, String> pods) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("stability of rolling update will be not triggered", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
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
            LOGGER.debug("Some pods still need to roll: {}", map);
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
            Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(expectedPods), () -> depHasRolled(name, snapshot));
        waitForDeploymentReady(name);
        PodUtils.waitForPodsReady(kubeClient().getDeployment(name).getSpec().getSelector(), expectedPods, true);
        LOGGER.info("Deployment {} rolling update finished", name);
        return depSnapshot(name);
    }

    /**
     * Wait until the given Deployment has been recovered.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentRecovery(String name, String deploymentUid) {
        LOGGER.info("Waiting for Deployment {}-{} recovery in namespace {}", name, deploymentUid, kubeClient().getNamespace());
        TestUtils.waitFor("deployment " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !kubeClient().getDeploymentUid(name).equals(deploymentUid));
        LOGGER.info("Deployment {} was recovered", name);
    }

    public static boolean waitForDeploymentReady(String deploymentName) {
        LOGGER.info("Wait for Deployment: {} will be ready", deploymentName);

        TestUtils.waitFor(String.format("Wait for Deployment: %s will be ready", deploymentName),
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient().getDeploymentStatus(deploymentName),
            () -> DeploymentUtils.logCurrentDeploymentStatus(kubeClient().getDeployment(deploymentName)));

        LOGGER.info("Deployment: {} is ready", deploymentName);
        return true;
    }

    /**
     * Wait until the given Deployment is ready.
     * @param deploymentName The name of the Deployment.
     * @param expectPods The expected number of pods.
     */
    public static boolean waitForDeploymentAndPodsReady(String deploymentName, int expectPods) {
        waitForDeploymentReady(deploymentName);

        LOGGER.info("Waiting for {} Pod(s) of Deployment {} to be ready", expectPods, deploymentName);
        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(deploymentName), expectPods, true,
            () -> DeploymentUtils.logCurrentDeploymentStatus(kubeClient().getDeployment(deploymentName)));
        LOGGER.info("Deployment {} is ready", deploymentName);
        return true;
    }

    /**
     * Wait until the given Deployment has been deleted.
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentDeletion(String name) {
        LOGGER.debug("Waiting for Deployment {} deletion", name);
        TestUtils.waitFor("Deployment " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                if (kubeClient().getDeployment(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment {} is not deleted yet! Triggering force delete by cmd client!", name);
                    cmdKubeClient().deleteByName(Constants.DEPLOYMENT, name);
                    return false;
                }
            });
        LOGGER.debug("Deployment {} was deleted", name);
    }
}
