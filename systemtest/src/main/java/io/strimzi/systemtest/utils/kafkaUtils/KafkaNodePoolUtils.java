/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class KafkaNodePoolUtils {

    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();
    private static final Logger LOGGER = LogManager.getLogger(PodUtils.class);

    private KafkaNodePoolUtils() {}

    public static KafkaNodePool getKafkaNodePool(String namespace, String resourceName) {
        return KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespace).withName(resourceName).get();
    }

    public static List<Integer> getCurrentKafkaNodePoolIds(String namespace, String resourceName) {
        return getKafkaNodePool(namespace, resourceName).getStatus().getNodeIds();
    }

    public static void setKafkaNodePoolAnnotation(String namespace, String resourceName,  Map<String, String> annotations) {
        LOGGER.info("Annotating KafkaNodePool: {}/{} with annotation: {}", namespace, resourceName, annotations);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(namespace, resourceName,
            kafkaNodePool -> kafkaNodePool.getMetadata().setAnnotations(annotations));
    }

    public static void scaleKafkaNodePool(String namespace, String kafkaNodePoolName, int scaleToReplicas) {
        LOGGER.info("Scaling KafkaNodePool: {}/{} to {} replicas", namespace, kafkaNodePoolName, scaleToReplicas);
        KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespace).withName(kafkaNodePoolName).scale(scaleToReplicas);
    }

    public static void deleteKafkaNodePoolWithPodSetAndWait(String namespace, String kafkaClusterName, String kafkaNodePoolName) {
        LOGGER.info("Waiting for deletion of KafkaNodePool: {}/{}", namespace, kafkaNodePoolName);
        TestUtils.waitFor("deletion of KafkaNodePool: " + namespace + "/" + kafkaNodePoolName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespace).withName(kafkaNodePoolName).get() == null &&
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespace).withName(kafkaClusterName + "-" + kafkaNodePoolName).get() == null
                ) {
                    return true;
                } else {
                    cmdKubeClient(namespace).deleteByName(KafkaNodePool.RESOURCE_KIND, kafkaNodePoolName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespace).withName(kafkaNodePoolName).get()));
    }

    public static void waitForKafkaNodePoolPodsReady(TestStorage testStorage, String kafkaNodePoolName, ProcessRoles nodePoolRole, int replicaCount) {
        waitForKafkaNodePoolPodsReady(
            testStorage.getNamespace(),
            testStorage.getClusterName(),
            kafkaNodePoolName,
            replicaCount,
            nodePoolRole
        );
    }

    public static void waitForKafkaNodePoolPodsReady(String namespace, String kafkaClusterName, ProcessRoles nodePoolRole, String kafkaNodePoolName, int replicaCount) {
        waitForKafkaNodePoolPodsReady(
            namespace,
            kafkaClusterName,
            kafkaNodePoolName,
            replicaCount,
            nodePoolRole
        );
    }

    public static void waitForKafkaNodePoolPodsReady(String namespace, String kafkaClusterName, String kafkaNodePoolName, int podReplicaCount, ProcessRoles processRoles) {
        LOGGER.info("Waiting for pods and SPS of KafkaNodePool: {}/{} to be ready", namespace, kafkaNodePoolName);
        final LabelSelector kNPPodslabelSelector = KafkaNodePoolResource.getLabelSelector(kafkaClusterName, kafkaNodePoolName, processRoles);
        PodUtils.waitForPodsReady(namespace, kNPPodslabelSelector, podReplicaCount, false);
    }

    /**
     * Waits for the KafkaNodePool Status to be updated after changed. It checks the generation and observed generation to
     * ensure the status is up to date.
     *
     * @param namespace     Namespace name
     * @param nodePoolName      Name of the KafkaNodePool cluster which should be checked
     */
    public static void waitForKafkaNodePoolStatusUpdate(String namespace, String nodePoolName) {
        LOGGER.info("Waiting for KafkaNodePool status to be updated");
        TestUtils.waitFor("Kafka status to be updated", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            KafkaNodePool k = KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespace).withName(nodePoolName).get();
            return k.getMetadata().getGeneration() == k.getStatus().getObservedGeneration();
        });
    }
}
