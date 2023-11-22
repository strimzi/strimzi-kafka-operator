/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ConfigMapUtils {

    private static final Logger LOGGER = LogManager.getLogger(ConfigMapUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private ConfigMapUtils() { }

    /**
     * Wait until the config map has been recovered.
     * @param name The name of the ConfigMap.
     */
    public static void waitForConfigMapRecovery(String namespaceName, String name, String configMapUid) {
        LOGGER.info("Waiting for ConfigMap: {}/{}-{} recovery", namespaceName, name, configMapUid);
        TestUtils.waitFor("recovery of ConfigMap: " + namespaceName + "/" + name, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !kubeClient().getConfigMapUid(name).equals(configMapUid));
        LOGGER.info("ConfigMap: {}/{} was recovered", namespaceName, name);
    }

    public static void waitForConfigMapLabelsChange(String namespaceName, String configMapName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for ConfigMap: {}/{} label to change {} -> {}", namespaceName, configMapName, entry.getKey(), entry.getValue());
                TestUtils.waitFor("ConfigMap label to change " + entry.getKey() + " -> " + entry.getValue(), TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    TestConstants.GLOBAL_TIMEOUT, () ->
                        kubeClient(namespaceName).getConfigMap(namespaceName, configMapName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

}
