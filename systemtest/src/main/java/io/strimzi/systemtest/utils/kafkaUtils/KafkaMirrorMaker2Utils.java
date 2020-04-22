/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaMirrorMaker2Utils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2Utils.class);

    private KafkaMirrorMaker2Utils() {}

    public static void waitUntilKafkaMirrorMaker2Status(String clusterName, String state) {
        LOGGER.info("Waiting till Kafka MirrorMaker2 CR will be in state: {}", state);
        TestUtils.waitFor("Waiting for Kafka MirrorMaker2 resource status is: " + state, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(kubeClient().getNamespace())
                .withName(clusterName).get().getStatus().getConditions().get(0).getType().equals(state)
        );
        LOGGER.info("Kafka MirrorMaker2 CR is in state: {}", state);
    }
}
