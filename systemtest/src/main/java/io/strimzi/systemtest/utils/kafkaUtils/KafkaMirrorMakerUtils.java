/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaMirrorMakerUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMakerUtils.class);

    private KafkaMirrorMakerUtils() {}

    /**
     * Wait until KafkaMirrorMaker status is in desired state
     * @param clusterName name of KafkaMirrorMaker cluster
     * @param state desired state - like Ready
     */
    public static void waitForKafkaMirrorMakerStatus(String clusterName, String state) {
        LOGGER.info("Wait until KafkaMirrorMaker CR will be in state: {}", state);
        TestUtils.waitFor("Waiting for Kafka resource status is: " + state, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getStatus().getConditions().get(0).getType().equals(state),
            () -> StUtils.logCurrentStatus(KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get()));
        LOGGER.info("KafkaMirrorMaker CR is in state: {}", state);
    }

    public static void waitForKafkaMirrorMakerReady(String clusterName) {
        waitForKafkaMirrorMakerStatus(clusterName, "Ready");
    }

    public static void waitForKafkaMirrorMakerNotReady(String clusterName) {
        waitForKafkaMirrorMakerStatus(clusterName, "Ready");
    }
}
