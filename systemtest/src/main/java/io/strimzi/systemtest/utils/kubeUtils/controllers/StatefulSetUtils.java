/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class StatefulSetUtils {

    private static final Logger LOGGER = LogManager.getLogger(StatefulSetUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(TestConstants.STATEFUL_SET);

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

        LOGGER.info("Waiting for StatefulSet: {}/{} to be ready", namespaceName, statefulSetName);
        TestUtils.waitFor("readiness of StatefulSet: " + namespaceName + "/" + statefulSetName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, timeout,
            () -> kubeClient(namespaceName).getStatefulSetStatus(namespaceName, statefulSetName),
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));

        LOGGER.info("Waiting for {} Pod(s) of StatefulSet: {}/{} to be ready", expectPods, namespaceName, statefulSetName);
        PodUtils.waitForPodsReady(namespaceName, kubeClient(namespaceName).getStatefulSetSelectors(namespaceName, statefulSetName), expectPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));
        LOGGER.info("StatefulSet: {}/{} is ready", namespaceName, statefulSetName);
    }

    public static void waitForAllStatefulSetPodsReady(String namespaceName, String statefulSetName, int expectPods) {
        waitForAllStatefulSetPodsReady(namespaceName, statefulSetName, expectPods, READINESS_TIMEOUT);
    }
}
