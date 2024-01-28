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

    public static KafkaNodePool getKafkaNodePool(String namespaceName, String resourceName) {
        return KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(resourceName).get();
    }

    public static List<Integer> getCurrentKafkaNodePoolIds(String namespaceName, String resourceName) {
        return getKafkaNodePool(namespaceName, resourceName).getStatus().getNodeIds();
    }

    public static void setKafkaNodePoolAnnotation(String namespaceName, String resourceName,  Map<String, String> annotations) {
        LOGGER.info("Annotating KafkaNodePool: {}/{} with annotation: {}", namespaceName, resourceName, annotations);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(resourceName,
            kafkaNodePool -> kafkaNodePool.getMetadata().setAnnotations(annotations),  namespaceName);
    }

    public static void scaleKafkaNodePool(String namespaceName, String kafkaNodePoolName, int scaleToReplicas) {
        LOGGER.info("Scaling KafkaNodePool: {}/{} to {} replicas", namespaceName, kafkaNodePoolName, scaleToReplicas);
        KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(kafkaNodePoolName).scale(scaleToReplicas);
    }

    public static void deleteKafkaNodePoolWithPodSetAndWait(String namespaceName, String kafkaClusterName, String kafkaNodePoolName) {
        LOGGER.info("Waiting for deletion of KafkaNodePool: {}/{}", namespaceName, kafkaNodePoolName);
        TestUtils.waitFor("deletion of KafkaNodePool: " + namespaceName + "/" + kafkaNodePoolName, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(kafkaNodePoolName).get() == null &&
                    StrimziPodSetResource.strimziPodSetClient().inNamespace(namespaceName).withName(kafkaClusterName + "-" + kafkaNodePoolName).get() == null
                ) {
                    return true;
                } else {
                    cmdKubeClient(namespaceName).deleteByName(KafkaNodePool.RESOURCE_KIND, kafkaNodePoolName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(kafkaNodePoolName).get()));
    }

    public static void waitForKafkaNodePoolPodsReady(TestStorage testStorage, String kafkaNodePoolName, ProcessRoles nodePoolRole, int replicaCount) {
        waitForKafkaNodePoolPodsReady(
            testStorage.getNamespaceName(),
            testStorage.getClusterName(),
            kafkaNodePoolName,
            replicaCount,
            nodePoolRole
        );
    }

    public static void waitForKafkaNodePoolPodsReady(String namespaceName, String kafkaClusterName, ProcessRoles nodePoolRole, String kafkaNodePoolName, int replicaCount) {
        waitForKafkaNodePoolPodsReady(
            namespaceName,
            kafkaClusterName,
            kafkaNodePoolName,
            replicaCount,
            nodePoolRole
        );
    }

    public static void waitForKafkaNodePoolPodsReady(String namespaceName, String kafkaClusterName, String kafkaNodePoolName, int podReplicaCount, ProcessRoles processRoles) {
        LOGGER.info("Waiting for pods and SPS of KafkaNodePool: {}/{} to be ready", namespaceName, kafkaNodePoolName);
        final LabelSelector kNPPodslabelSelector = KafkaNodePoolResource.getLabelSelector(kafkaClusterName, kafkaNodePoolName, processRoles);
        PodUtils.waitForPodsReady(namespaceName, kNPPodslabelSelector, podReplicaCount, false);
    }

    /**
     * Waits for the KafkaNodePool Status to be updated after changed. It checks the generation and observed generation to
     * ensure the status is up to date.
     *
     * @param namespaceName     Namespace name
     * @param nodePoolName      Name of the KafkaNodePool cluster which should be checked
     */
    public static void waitForKafkaNodePoolStatusUpdate(String namespaceName, String nodePoolName) {
        LOGGER.info("Waiting for KafkaNodePool status to be updated");
        TestUtils.waitFor("Kafka status to be updated", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            KafkaNodePool k = KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(nodePoolName).get();
            return k.getMetadata().getGeneration() == k.getStatus().getObservedGeneration();
        });
    }
}
