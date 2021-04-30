/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class DeploymentConfigUtils {

    private static final Logger LOGGER = LogManager.getLogger(DeploymentConfigUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.DEPLOYMENT_CONFIG);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    /**
     * Returns a map of pod name to resource version for the pods currently in the given DeploymentConfig.
     * @param namespaceName Namespace name
     * @param name The DeploymentConfig name.
     * @return A map of pod name to resource version for pods in the given DeploymentConfig.
     */
    public static Map<String, String> depConfigSnapshot(String namespaceName, String name) {
        LabelSelector selector = new LabelSelectorBuilder().addToMatchLabels(kubeClient(namespaceName).getDeploymentConfigSelectors(namespaceName, name)).build();
        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static Map<String, String> depConfigSnapshot(String name) {
        return depConfigSnapshot(kubeClient().getNamespace(), name);
    }

    /**
     * Method to check that all pods for expected DeploymentConfig were rolled
     * @param namespaceName Namespace name
     * @param name DeploymentConfig name
     * @param snapshot Snapshot of pods for DeploymentConfig before the rolling update
     * @return true when the pods for DeploymentConfig are recreated
     */
    public static boolean depConfigHasRolled(String namespaceName, String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        LabelSelector selector = new LabelSelectorBuilder().addToMatchLabels(kubeClient(namespaceName).getDeploymentConfigSelectors(namespaceName, name)).build();
        Map<String, String> map = PodUtils.podSnapshot(namespaceName, selector);
        LOGGER.debug("Current  snapshot: {}", new TreeMap<>(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.info("All pods seem to have rolled");
            return true;
        } else {
            LOGGER.debug("Some pods still need to roll: {}", map);
            return false;
        }
    }

    public static boolean depConfigHasRolled(String name, Map<String, String> snapshot) {
        return depConfigHasRolled(kubeClient().getNamespace(), name, snapshot);
    }

    /**
     * Method to wait when DeploymentConfig will be recreated after rolling update
     * @param namespaceName Namespace name
     * @param depConfigName DeploymentConfig name
     * @param snapshot Snapshot of pods for DeploymentConfig before the rolling update
     * @return The snapshot of the DeploymentConfig after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillDepConfigHasRolled(String namespaceName, String depConfigName, Map<String, String> snapshot) {
        LOGGER.info("Waiting for DeploymentConfig {} rolling update", depConfigName);
        TestUtils.waitFor("DeploymentConfig roll of " + depConfigName,
            Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(snapshot.size()), () -> depConfigHasRolled(namespaceName, depConfigName, snapshot));
        waitForDeploymentConfigReady(namespaceName, depConfigName);
        LOGGER.info("DeploymentConfig {} rolling update finished", depConfigName);
        return depConfigSnapshot(namespaceName, depConfigName);
    }

    public static Map<String, String> waitTillDepConfigHasRolled(String depConfigName, Map<String, String> snapshot) {
        return waitTillDepConfigHasRolled(kubeClient().getNamespace(), depConfigName, snapshot);
    }

    /**
     * Wait until the given DeploymentConfig has been deleted.
     * @param namespaceName Namespace name
     * @param name The name of the DeploymentConfig.
     */
    public static void waitForDeploymentConfigDeletion(String namespaceName, String name) {
        LOGGER.debug("Waiting for DeploymentConfig {} deletion", name);
        TestUtils.waitFor("DeploymentConfig " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                if (kubeClient(namespaceName).getDeploymentConfig(name) == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment {} is not deleted yet! Triggering force delete by cmd client!", name);
                    cmdKubeClient(namespaceName).deleteByName("deploymentconfig", name);
                    return false;
                }
            });
        LOGGER.debug("DeploymentConfig {} was deleted", name);
    }

    public static void waitForDeploymentConfigDeletion(String name) {
        waitForDeploymentConfigDeletion(kubeClient().getNamespace(), name);
    }

    /**
     * Wait until the given DeploymentConfig is ready.
     * @param namespaceName Namespace name
     * @param depConfigName The name of the DeploymentConfig.
     */
    public static Map<String, String> waitForDeploymentConfigAndPodsReady(String namespaceName, String depConfigName, int expectPods) {
        waitForDeploymentConfigReady(namespaceName, depConfigName);

        LOGGER.info("Waiting for Pod(s) of DeploymentConfig {} to be ready", depConfigName);

        LabelSelector deploymentConfigSelector =
            new LabelSelectorBuilder().addToMatchLabels(kubeClient(namespaceName).getDeploymentConfigSelectors(namespaceName, depConfigName)).build();

        PodUtils.waitForPodsReady(namespaceName, deploymentConfigSelector, expectPods, true);
        LOGGER.info("DeploymentConfig {} is ready", depConfigName);

        return depConfigSnapshot(namespaceName, depConfigName);
    }

    public static Map<String, String> waitForDeploymentConfigAndPodsReady(String depConfigName, int expectPods) {
        return waitForDeploymentConfigAndPodsReady(kubeClient().getNamespace(), depConfigName, expectPods);
    }

    public static void waitForDeploymentConfigReady(String namespaceName, String depConfigName) {
        LOGGER.info("Wait for DeploymentConfig: {} will be ready", depConfigName);

        TestUtils.waitFor(String.format("Wait for DeploymentConfig: %s will be ready", depConfigName),
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient(namespaceName).getDeploymentConfigReadiness(namespaceName, depConfigName),
            () -> {
                if (kubeClient(namespaceName).getDeploymentConfig(namespaceName, depConfigName) != null) {
                    LOGGER.info(kubeClient(namespaceName).getDeploymentConfig(namespaceName, depConfigName));
                }
            });

        LOGGER.info("Wait for DeploymentConfig: {} is ready", depConfigName);
    }

    public static void waitForDeploymentConfigReady(String depConfigName) {
        LOGGER.info("Wait for DeploymentConfig: {} will be ready", depConfigName);

        TestUtils.waitFor(String.format("Wait for DeploymentConfig: %s will be ready", depConfigName),
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient().getDeploymentConfigReadiness(depConfigName),
            () -> {
                if (kubeClient().getDeploymentConfig(depConfigName) != null) {
                    LOGGER.info(kubeClient().getDeploymentConfig(depConfigName));
                }
            });

        LOGGER.info("Wait for DeploymentConfig: {} is ready", depConfigName);
    }
}
