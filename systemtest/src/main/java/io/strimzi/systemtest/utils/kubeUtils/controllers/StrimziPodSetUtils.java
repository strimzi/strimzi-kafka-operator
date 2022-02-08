/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class StrimziPodSetUtils {

    private StrimziPodSetUtils() {}

    private static final Logger LOGGER = LogManager.getLogger(StatefulSetUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(StrimziPodSet.RESOURCE_KIND);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Pod getFirstPodFromSpec(String namespaceName, String resourceName) {
        Map<String, Object> podMap = StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName)
            .get().getSpec().getPods().stream().findFirst().get();

        return mapToPod(podMap);
    }

    public static void waitForStrimziPodSetLabelsDeletion(String namespaceName, String resourceName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for StrimziPodSet label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for StrimziPodSet label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("StrimziPodSet label {} change to {}", labelKey, null);
        }
    }

    public static void waitForStrimziPodSetLabelsChange(String namespaceName, String resourceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for StrimziPodSet set label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for StrimziPodSet label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.GLOBAL_TIMEOUT, () ->
                        StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
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
