/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.Crds;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    private KafkaUtils() {}

    public static void waitUntilKafkaCRIsReady(String clusterName) {
        LOGGER.info("Waiting till Kafka CR will be ready");
        TestUtils.waitFor("Waiting for Kafka resource status is ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->   Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0).getType().equals("Ready") &&
                Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0).getStatus().equals("True")
        );
        LOGGER.info("Kafka CR will be ready");
    }

    public static void waitUntilKafkaStatusConditionIsPresent(String clusterName) {
        LOGGER.info("Waiting till kafka resource status is present");
        TestUtils.waitFor("Waiting for Kafka resource status is ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->  Crds.kafkaOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0) != null
        );
        LOGGER.info("Kafka resource status is present");
    }
}
