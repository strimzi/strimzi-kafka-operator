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
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

public class DeploymentUtils {

    private static final Logger LOGGER = LogManager.getLogger(DeploymentUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(TestConstants.DEPLOYMENT);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private DeploymentUtils() { }

    /**
     * Replaces Deployment in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName  name of the Namespace where the resource should be replaced.
     * @param resourceName   name of the Deployment's name.
     * @param editor         editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<Deployment> editor) {
        Deployment toBeReplaced = KubeResourceManager.get().kubeClient().getClient().resources(Deployment.class, DeploymentList.class).inNamespace(namespaceName).withName(resourceName).get();
        editor.accept(toBeReplaced);
        KubeResourceManager.get().kubeClient().getClient().resources(Deployment.class, DeploymentList.class).inNamespace(namespaceName).resource(toBeReplaced).update();
    }

    /**
     * Log actual status of deployment with pods
     * @param deployment - every Deployment, that HasMetadata and has status (fabric8 status)
     **/
    public static void logCurrentDeploymentStatus(String namespaceName, Deployment deployment) {
        if (deployment != null) {
            String kind = deployment.getKind();
            String name = deployment.getMetadata().getName();

            List<String> log = new ArrayList<>(asList("\n", kind, " status:\n", "\nConditions:\n"));

            for (DeploymentCondition deploymentCondition : deployment.getStatus().getConditions()) {
                log.add("\tType: " + deploymentCondition.getType() + "\n");
                log.add("\tMessage: " + deploymentCondition.getMessage() + "\n");
            }

            if (KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespaceName, name).size() != 0) {
                log.add("\nPods with conditions and messages:\n\n");

                for (Pod pod : KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespaceName, name)) {
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

    public static void waitForNoRollingUpdate(String namespaceName, String deploymentName, Map<String, String> pods) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Deployment to remain stable and rolling update not to be triggered", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                if (!DeploymentUtils.depHasRolled(namespaceName, deploymentName, pods)) {
                    LOGGER.info("{}/{} Pod(s) not rolling. Must remain stable for: {} second(s)", namespaceName, pods.toString(),
                        TestConstants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == TestConstants.GLOBAL_RECONCILIATION_COUNT;
                } else {
                    throw new RuntimeException(pods.toString() + " Pod(s) are rolling!");
                }
            }
        );
    }

    /**
     * Returns a map of pod name to resource version for the Pods currently in the given deployment.
     * @param name The Deployment name.
     * @return A map of pod name to resource version for Pods in the given Deployment.
     */
    public static Map<String, String> depSnapshot(String namespaceName, String name) {
        Deployment deployment = KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(name).get();
        LabelSelector selector = deployment.getSpec().getSelector();
        return PodUtils.podSnapshot(namespaceName, selector);
    }

    /**
     * Method to check that all Pods for expected Deployment were rolled
     * @param namespaceName Namespace name
     * @param name Deployment name
     * @param snapshot Snapshot of Pods for Deployment before the rolling update
     * @return true when the Pods for Deployment are recreated
     */
    public static boolean depHasRolled(String namespaceName, String name, Map<String, String> snapshot) {
        LOGGER.debug("Existing snapshot: {}/{}", namespaceName, new TreeMap<>(snapshot));
        Map<String, String> map = PodUtils.podSnapshot(namespaceName, KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(name).get().getSpec().getSelector());
        LOGGER.debug("Current  snapshot: {}/{}", namespaceName, new TreeMap<>(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.debug("All Pods seem to have rolled");
            return true;
        } else {
            LOGGER.debug("Some Pods still need to roll: {}/{}", namespaceName, map);
            return false;
        }
    }

    /**
     * Method to wait when Deployment will be recreated after rolling update
     * @param namespaceName Namespace name where pod of the deployment is located
     * @param name Deployment name
     * @param expectedPods Expected number of Pods
     * @param snapshot Snapshot of Pods for Deployment before the rolling update
     * @return The snapshot of the Deployment after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillDepHasRolled(String namespaceName, String name, int expectedPods, Map<String, String> snapshot) {
        Map<String, String> newDepSnapshot = waitTillDepHasRolled(namespaceName, name, snapshot);
        waitForDeploymentAndPodsReady(namespaceName, name, expectedPods);

        LOGGER.info("Deployment: {}/{} rolling update finished", namespaceName, name);
        return newDepSnapshot;
    }

    public static Map<String, String> waitTillDepHasRolled(String namespaceName, String deploymentName, Map<String, String> snapshot) {
        LOGGER.info("Waiting for Deployment: {}/{} rolling update", namespaceName, deploymentName);
        TestUtils.waitFor("rolling update of Deployment " + namespaceName + "/" + deploymentName,
            TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(snapshot.size()),
                () -> depHasRolled(namespaceName, deploymentName, snapshot));

        return depSnapshot(namespaceName, deploymentName);
    }

    public static boolean waitForDeploymentReady(String namespaceName, String deploymentName) {
        LOGGER.info("Waiting for Deployment: {}/{} to be ready", namespaceName, deploymentName);

        TestUtils.waitFor("readiness of Deployment: " + namespaceName + "/" + deploymentName,
            TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(deploymentName).isReady(),
            () -> DeploymentUtils.logCurrentDeploymentStatus(namespaceName, KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(deploymentName).get()));

        LOGGER.info("Deployment: {}/{} is ready", namespaceName, deploymentName);
        return true;
    }

    /**
     * Wait until the given Deployment is ready.
     * @param namespaceName name of the namespace
     * @param deploymentName The name of the Deployment.
     * @param expectPods The expected number of Pods.
     */
    public static boolean waitForDeploymentAndPodsReady(String namespaceName, String deploymentName, int expectPods) {
        waitForDeploymentReady(namespaceName, deploymentName);

        LOGGER.info("Waiting for {} Pod(s) of Deployment: {}/{} to be ready", expectPods, namespaceName, deploymentName);
        PodUtils.waitForPodsReady(namespaceName, KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(deploymentName).get().getSpec().getSelector(), expectPods, true,
            () -> DeploymentUtils.logCurrentDeploymentStatus(namespaceName, KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(deploymentName).get()));
        LOGGER.info("Deployment: {}/{} is ready", namespaceName, deploymentName);
        return true;
    }

    /**
     * Wait until the given Deployment has been deleted.
     * @param namespaceName Namespace name
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentDeletion(String namespaceName, String name) {
        LOGGER.debug("Waiting for Deployment: {}/{} deletion", namespaceName, name);
        TestUtils.waitFor("deletion of Deployment: " + namespaceName + "/" + name, TestConstants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                if (KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(name).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("Deployment: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespaceName, name);
                    KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteByName(TestConstants.DEPLOYMENT, name);
                    return false;
                }
            });
        LOGGER.debug("Deployment: {}/{} was deleted", namespaceName, name);
    }

    public static void waitForCreationOfDeploymentWithPrefix(String namespaceName, String deploymentNamePrefix) {
        TestUtils.waitFor(String.format("creation of Deployment with prefix: %s in Namespace: %s", deploymentNamePrefix, namespaceName), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getDeploymentNameByPrefix(namespaceName, deploymentNamePrefix) != null);
    }
}
