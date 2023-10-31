/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Affinity;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class StrimziPodSetUtils {

    private StrimziPodSetUtils() {}

    private static final Logger LOGGER = LogManager.getLogger(StrimziPodSetUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.STATEFUL_SET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(StrimziPodSet.RESOURCE_KIND);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Pod getFirstPodFromSpec(String namespaceName, String resourceName) {
        Map<String, Object> podMap = StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName)
            .get().getSpec().getPods().stream().findFirst().get();

        return mapToPod(podMap);
    }

    public static void waitForStrimziPodSetLabelsDeletion(String namespaceName, String resourceName, Collection<String> labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for StrimziPodSet: {}/{} to change label: {} -> {}", namespaceName, resourceName, labelKey, null);
            TestUtils.waitFor("StrimziPodSet: " + namespaceName + "/" + resourceName + " to change label: " + labelKey + " -> " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("StrimziPodSet: {}/{} changed label: {} -> {}", namespaceName, resourceName, labelKey, null);
        }
    }

    /**
     * Waits until the single specified annotation of a Pod changes its value.
     *
     * @param namespaceName    the namespace in which the Pod is located.
     * @param podName          the name of the Pod whose annotation value needs to be checked.
     * @param strimziPodSetName the name of the SPS whose Pod's annotation is being observed.
     * @param annotationKey    the key of the annotation whose value needs to be checked.
     * @param annotationValue  the expected value of the annotation to change from.
     */
    public static void waitUntilPodAnnotationChange(String namespaceName, String podName, String strimziPodSetName, String annotationKey, String annotationValue) {

        LOGGER.info("Waiting for Pod: {}/{} to change annotation value: {}-{}", namespaceName, podName, annotationKey, annotationValue);
        final ObjectMapper objectMapper = new ObjectMapper();


        TestUtils.waitFor("Pod to change value of annotation" + annotationKey + " with key " + annotationKey, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
            DELETION_TIMEOUT, () -> {
                // parse pod from SPS
                StrimziPodSet sps = StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(strimziPodSetName).get();
                var podMap = sps.getSpec().getPods();
                Pod[] spsPods = objectMapper.convertValue(podMap, Pod[].class);
                Pod pod = Arrays.stream(spsPods).filter(p -> p.getMetadata().getName().contains(podName)).findAny().get();

                return !annotationValue.equals(pod.getMetadata().getAnnotations().get(annotationKey));
            });
        LOGGER.info("Pod: {}/{} changed annotation value: {}-{}", namespaceName, podName, annotationKey, annotationValue);
    }

    /**
     * Waits for a specific annotation key-value pair to remain stable (from start to beginning) on a given Pod.
     *
     * @param namespaceName   the namespace in which the Pod is located.
     * @param annotationKey   the key of the annotation to be checked.
     * @param annotationValue the expected value of the annotation to be checked.
     * @param strimziPodSetName the name of the SPS whose Pod's annotation is being observed.
     * @param podName the name of the Pod whose annotation is being observed.
     */
    public static void waitForPrevailedPodAnnotationKeyValuePairs(String namespaceName, String annotationKey, String annotationValue, String strimziPodSetName, String podName) {
        LOGGER.info("Waiting for label: {}-{} to keep its value in namespace: {}", namespaceName, annotationKey, annotationValue);
        int[] stableCounter = {0};
        final ObjectMapper objectMapper = new ObjectMapper();

        TestUtils.waitFor("Label: " + annotationKey + "to prevail its value: " + annotationValue,
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,

            () -> {
                // parse pod from SPS
                StrimziPodSet sps =  StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(strimziPodSetName).get();
                var podMap = sps.getSpec().getPods();

                Pod[] spsPods = objectMapper.convertValue(podMap, Pod[].class);
                Pod pod = Arrays.stream(spsPods).filter(p -> p.getMetadata().getName().contains(podName)).findAny().get();


                if (annotationValue.equals(pod.getMetadata().getAnnotations().get(annotationKey))) {
                    stableCounter[0]++;
                    if (stableCounter[0] == Constants.GLOBAL_STABILITY_OFFSET_COUNT) {
                        LOGGER.info("Pod replicas are stable for {} poll intervals", stableCounter[0]);
                        return true;
                    }
                } else {
                    LOGGER.info("Annotations does not have expected value");
                    return false;
                }
                LOGGER.info("Annotation be assumed stable in {} polls", Constants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
                return false;
            });
        LOGGER.info("Pod: {}/{} kept annotation: {}-{} ", namespaceName, podName, annotationKey, annotationValue);
    }


    /**
     * Waits for all of specified labels of a StrimziPodSet (SPS) resource to change.
     *
     * @param namespaceName  the namespace in which the SPS is located.
     * @param resourceName   the name of the SPS resource.
     * @param labels         a map containing key-value pairs of the labels names amd values.
     */
    public static void waitForStrimziPodSetLabelsChange(String namespaceName, String resourceName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for StrimziPodSet: {}/{} to change label: {} -> {}", namespaceName, resourceName, entry.getKey(), entry.getValue());
                TestUtils.waitFor("StrimziPodSet: " + namespaceName + "/" + resourceName + " to change label: " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
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
    public static void waitForAllStrimziPodSetAndPodsReady(String namespaceName, String spsName, String componentName, int expectPods, long timeout) {
        String resourceName = componentName.contains("-kafka") ? componentName.replace("-kafka", "") : componentName.replace("-zookeeper", "");
        LabelSelector labelSelector = KafkaResource.getLabelSelector(resourceName, componentName);

        LOGGER.info("Waiting for StrimziPodSet: {}/{} to be ready", namespaceName, spsName);
        TestUtils.waitFor("readiness of StrimziPodSet: " + namespaceName + "/" + spsName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, timeout,
            () -> {
                StrimziPodSetStatus podSetStatus = StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(spsName).get().getStatus();
                return podSetStatus.getPods() == podSetStatus.getReadyPods();
            },
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));

        LOGGER.info("Waiting for {} Pod(s) of StrimziPodSet {}/{} to be ready", expectPods, namespaceName, spsName);
        PodUtils.waitForPodsReady(namespaceName, labelSelector, expectPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));
        LOGGER.info("StrimziPodSet: {}/{} is ready", namespaceName, spsName);
    }

    public static void waitForAllStrimziPodSetAndPodsReady(String namespaceName, String spsName, String componentName, int expectPods) {
        waitForAllStrimziPodSetAndPodsReady(namespaceName, spsName, componentName, expectPods, READINESS_TIMEOUT);
    }

    /**
     * Wait until the given StrimziPodSet has been recovered.
     * @param resourceName The name of the StrimziPodSet.
     */
    public static void waitForStrimziPodSetRecovery(String namespaceName, String resourceName, String resourceUID) {
        LOGGER.info("Waiting for StrimziPodSet: {}/{}-{} recovery", namespaceName, resourceName, resourceUID);
        TestUtils.waitFor("readiness of StrimziPodSet: " + namespaceName + "/" + resourceName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getUid().equals(resourceUID));
        LOGGER.info("StrimziPodSet: {}/{} was recovered", namespaceName, resourceName);
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

    public static void annotateStrimziPodSet(String namespaceName, String resourceName, Map<String, String> annotations) {
        StrimziPodSetResource.replaceStrimziPodSetInSpecificNamespace(resourceName,
            strimziPodSet -> strimziPodSet.getMetadata().setAnnotations(annotations), namespaceName);
    }

    public static Map<String, String> getAnnotationsOfStrimziPodSet(String namespaceName, String resourceName) {
        return StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getAnnotations();
    }

    public static Map<String, String> getLabelsOfStrimziPodSet(String namespaceName, String resourceName) {
        return StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getLabels();
    }

    public static Affinity getStrimziPodSetAffinity(String namespaceName, String resourceName) {
        Pod firstPod = StrimziPodSetUtils.getFirstPodFromSpec(namespaceName, resourceName);
        return firstPod.getSpec().getAffinity();
    }

    public static void deleteStrimziPodSet(String namespaceName, String resourceName) {
        StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).delete();
    }

    public static String getStrimziPodSetUID(String namespaceName, String resourceName) {
        return StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(resourceName).get().getMetadata().getUid();
    }
}
