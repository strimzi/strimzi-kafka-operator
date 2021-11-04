/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class StatefulSetUtils {

    private static final Logger LOGGER = LogManager.getLogger(StatefulSetUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.STATEFUL_SET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(Constants.STATEFUL_SET);

    private StatefulSetUtils() { }

    /**
     * Wait until the STS is ready and all of its Pods are also ready with custom timeout.
     *
     * @param namespaceName Namespace name
     * @param statefulSetName The name of the StatefulSet
     * @param expectPods The number of pods expected.
     */
    public static void waitForAllStatefulSetPodsReady(String namespaceName, String statefulSetName, int expectPods, long timeout) {
        String resourceName = statefulSetName.contains("-kafka") ? statefulSetName.replace("-kafka", "") : statefulSetName.replace("-zookeeper", "");

        LOGGER.info("Waiting for StatefulSet {} to be ready", statefulSetName);
        TestUtils.waitFor("StatefulSet " + statefulSetName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, timeout,
            () -> kubeClient(namespaceName).getStatefulSetStatus(namespaceName, statefulSetName),
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));

        LOGGER.info("Waiting for {} Pod(s) of StatefulSet {} to be ready", expectPods, statefulSetName);
        PodUtils.waitForPodsReady(namespaceName, kubeClient(namespaceName).getStatefulSetSelectors(namespaceName, statefulSetName), expectPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));
        LOGGER.info("StatefulSet {} is ready", statefulSetName);
    }

    public static void waitForAllStatefulSetPodsReady(String statefulSetName, int expectPods, long timeout) {
        waitForAllStatefulSetPodsReady(kubeClient().getNamespace(), statefulSetName, expectPods, timeout);
    }

    /**
     * Wait until the STS is ready and all of its Pods are also ready with default timeout.
     *
     * @param statefulSetName The name of the StatefulSet
     * @param expectPods The number of pods expected.
     */
    public static void waitForAllStatefulSetPodsReady(String statefulSetName, int expectPods) {
        waitForAllStatefulSetPodsReady(statefulSetName, expectPods, READINESS_TIMEOUT);
    }

    /**
     * Wait until the given StatefulSet has been recovered.
     * @param name The name of the StatefulSet.
     */
    public static void waitForStatefulSetRecovery(String name, String statefulSetUid) {
        LOGGER.info("Waiting for StatefulSet {}-{} recovery in namespace {}", name, statefulSetUid, kubeClient().getNamespace());
        TestUtils.waitFor("StatefulSet " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !kubeClient().getStatefulSetUid(name).equals(statefulSetUid));
        LOGGER.info("StatefulSet {} was recovered", name);
    }

    public static void waitForStatefulSetLabelsChange(String namespaceName, String statefulSetName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Stateful set label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for StatefulSet label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.GLOBAL_TIMEOUT, () ->
                        kubeClient(namespaceName).getStatefulSet(namespaceName, statefulSetName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForStatefulSetLabelsDeletion(String namespaceName, String statefulSetName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for StatefulSet label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for StatefulSet label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    kubeClient(namespaceName).getStatefulSet(namespaceName, statefulSetName).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("StatefulSet label {} change to {}", labelKey, null);
        }
    }
}
