/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.status.StrimziPodSetStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class StrimziPodSetUtils {

    private StrimziPodSetUtils() {}

    private static final Logger LOGGER = LogManager.getLogger(StatefulSetUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.STATEFUL_SET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(StrimziPodSet.RESOURCE_KIND);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Pod getFirstPodFromSpec(String namespaceName, String resourceName) {
        Map<String, Object> podMap = StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName)
            .get().getSpec().getPods().stream().findFirst().get();

        return mapToPod(podMap);
    }

    public static void waitForStrimziPodSetLabelsDeletion(String namespaceName, String resourceName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for StrimziPodSet {}/{} label {} change to {}", namespaceName, resourceName, labelKey, null);
            TestUtils.waitFor("StrimziPodSet " + namespaceName + "/" + resourceName + " label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("StrimziPodSet {}/{} label {} change to {}", namespaceName, resourceName, labelKey, null);
        }
    }

    public static void waitForStrimziPodSetLabelsChange(String namespaceName, String resourceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for StrimziPodSet {}/{} label change {} -> {}", namespaceName, resourceName, entry.getKey(), entry.getValue());
                TestUtils.waitFor("StrimziPodSet " + namespaceName + "/" + resourceName + " label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.GLOBAL_TIMEOUT, () ->
                        StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    /**
     * Wait until the SPS is ready and all of its Pods are also ready with custom timeout.
     *
     * @param namespaceName Namespace name
     * @param spsName The name of the StrimziPodSet
     * @param expectPods The number of pods expected.
     */
    public static void waitForAllStrimziPodSetAndPodsReady(String namespaceName, String spsName, int expectPods, long timeout) {
        String resourceName = spsName.contains("-kafka") ? spsName.replace("-kafka", "") : spsName.replace("-zookeeper", "");
        LabelSelector labelSelector = KafkaResource.getLabelSelector(resourceName, spsName);

        LOGGER.info("Waiting for StrimziPodSet {}/{} to be ready", namespaceName, spsName);
        TestUtils.waitFor("StrimziPodSet " + namespaceName + "/" + spsName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, timeout,
            () -> {
                StrimziPodSetStatus podSetStatus = StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(spsName).get().getStatus();
                return podSetStatus.getPods() == podSetStatus.getReadyPods();
            },
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));

        LOGGER.info("Waiting for {} Pod(s) of StrimziPodSet {}/{} to be ready", expectPods, namespaceName, spsName);
        PodUtils.waitForPodsReady(namespaceName, labelSelector, expectPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));
        LOGGER.info("StrimziPodSet {}/{} is ready", namespaceName, spsName);
    }

    public static void waitForAllStrimziPodSetAndPodsReady(String namespaceName, String spsName, int expectPods) {
        waitForAllStrimziPodSetAndPodsReady(namespaceName, spsName, expectPods, READINESS_TIMEOUT);
    }

    /**
     * Wait until the given StrimziPodSet has been recovered.
     * @param resourceName The name of the StrimziPodSet.
     */
    public static void waitForStrimziPodSetRecovery(String namespaceName, String resourceName, String resourceUID) {
        LOGGER.info("Waiting for StrimziPodSet {}/{}-{} recovery", namespaceName, resourceName, resourceUID);
        TestUtils.waitFor("StrimziPodSet " + namespaceName + "/" + resourceName + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getUid().equals(resourceUID));
        LOGGER.info("StrimziPodSet {}/{} was recovered", namespaceName, resourceName);
    }

    /**
     * Converts Map to Pod for decoding of StrimziPodSets
     *
     * @param map   Pod represented as Map which should be decoded
     * @return      Pod object decoded from the map
     */
    public static Pod mapToPod(Map<String, Object> map) {
        return MAPPER.convertValue(map, Pod.class);
    }
}
