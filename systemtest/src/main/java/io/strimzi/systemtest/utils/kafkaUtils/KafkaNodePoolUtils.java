/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaNodePoolUtils {

    private KafkaNodePoolUtils() {
    }

    private static final Logger LOGGER = LogManager.getLogger(PodUtils.class);

    public static KafkaNodePool getKafkaNodePool(String namespaceName, String resourceName) {
        return KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(resourceName).get();
    }

    public static List<Integer> getCurrentKafkaNodePoolIds(String namespaceName, String resourceName) {
        return getKafkaNodePool(namespaceName, resourceName).getStatus().getNodeIds();
    }

    public static String annotateKafkaNodePool(String namespaceName, String resourceName, String annotationKey, String annotationValue) {
        LOGGER.info("Annotating KafkaNodePool: {}/{} with annotation: {}", namespaceName, resourceName, annotationKey);
        return ResourceManager.cmdKubeClient().namespace(namespaceName).execInCurrentNamespace("annotate", "kafkanodepool", resourceName, annotationKey + "=" + annotationValue).out().trim();
    }

    // This method takes into account all possible annotation formats [0, 1, 2, 10-20, 30].
    public static String annotateKafkaNodePoolNextNodeIds(String namespaceName, String resourceName, String annotationValue) {
        return annotateKafkaNodePool(namespaceName, resourceName, Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, annotationValue);
    }

    // In case we want to set IDs for some reason in list of integers [0, 1, 2, 10, 30] format. This method does not represent all possibilities of this annotation.
    public static String annotateKafkaNodePoolNextNodeIds(String namespaceName, String resourceName, List<Integer> annotationValue) {
        return annotateKafkaNodePoolNextNodeIds(namespaceName, resourceName, annotationValue.toString());
    }

    // This method takes into account all possible annotation formats [0, 1, 2, 10-20, 30].
    public static String annotateKafkaNodePoolRemoveNodeIds(String namespaceName, String resourceName, String annotationValue) {
        return annotateKafkaNodePool(namespaceName, resourceName, Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, annotationValue.toString());
    }

    // In case we want to set IDs for some reason in list of integers [0, 1, 2, 10, 30] format. This method does not represent all possibilities of this annotation.
    public static String annotateKafkaNodePoolRemoveNodeIds(String namespaceName, String resourceName, List<Integer> annotationValue) {
        return annotateKafkaNodePoolRemoveNodeIds(namespaceName, resourceName, annotationValue.toString());
    }

    public static void waitForKafkaNodePoolStablePodReplicasCount(TestStorage testStorage, int expectedReplicas) {
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()), expectedReplicas);
    }

    public static void scaleKafkaNodePool(String namespaceName, String kafkaNodePoolName, int scaleToReplicas) {
        LOGGER.info("Scaling KafkaNodePool: {}/{} to {} replicas", namespaceName, kafkaNodePoolName, scaleToReplicas);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(kafkaNodePoolName, nodePool -> {
            nodePool.getSpec().setReplicas(scaleToReplicas);
        }, namespaceName);
    }

    // Format for parsing must be similar to this -> "[2, 3, 50-52, 88]" then it can return a list of integers [2,3,50,51,52,88]
    public static List<Integer> parseNodePoolIdsWithRangeStringToIntegerList(String inputString) {
        List<Integer> outputList = new ArrayList<>();
        inputString = inputString.replace("[", "").replace("]", "").replace(" ", "");
        String[] ranges = inputString.split(",");

        for (String range : ranges) {
            if (range.contains("-")) {
                String[] rangeValues = range.split("-");
                int start = Integer.parseInt(rangeValues[0]);
                int end = Integer.parseInt(rangeValues[1]);
                for (int i = start; i <= end; i++) {
                    outputList.add(i);
                }
            } else {
                outputList.add(Integer.parseInt(range));
            }
        }

        return outputList;
    }
}
