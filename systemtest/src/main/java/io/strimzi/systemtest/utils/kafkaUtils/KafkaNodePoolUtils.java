/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import java.util.List;
import java.util.Map;
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

    public static void setKafkaNodePoolAnnotation(String namespaceName, String resourceName,  Map<String, String> annotations) {
        LOGGER.info("Annotating KafkaNodePool: {}/{} with annotation: {}", namespaceName, resourceName, annotations);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(resourceName,
            kafkaNodePool -> kafkaNodePool.getMetadata().setAnnotations(annotations),  namespaceName);
    }

    public static void scaleKafkaNodePool(String namespaceName, String kafkaNodePoolName, int scaleToReplicas) {
        LOGGER.info("Scaling KafkaNodePool: {}/{} to {} replicas", namespaceName, kafkaNodePoolName, scaleToReplicas);
        KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(namespaceName).withName(kafkaNodePoolName).scale(scaleToReplicas);
    }
}
