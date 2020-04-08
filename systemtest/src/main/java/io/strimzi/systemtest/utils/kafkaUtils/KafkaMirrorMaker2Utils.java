/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaMirrorMaker2Utils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2Utils.class);
    private static String namespace = kubeClient().getNamespace();

    private KafkaMirrorMaker2Utils() {}

    /**
     * Wait until KafkaMirrorMaker2 will be in desired state
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static void waitForKafkaMirrorMaker2Status(String clusterName, String state) {
        LOGGER.info("Wait until KafkaMirrorMaker2 will be in state: {}", state);
        TestUtils.waitFor("KafkaMirrorMaker2 resource status is: " + state, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kafkaMirrorMaker2Client().inNamespace(namespace).withName(clusterName).get().getStatus().getConditions().get(0).getType().equals(state),
            () -> StUtils.logCurrentStatus(kafkaMirrorMaker2Client().inNamespace(namespace).withName(clusterName).get()));
        LOGGER.info("KafkaMirrorMaker2 is in state: {}", state);
    }

    public static void waitForKafkaMirrorMaker2IsReady(String clusterName) {
        waitForKafkaMirrorMaker2Status(clusterName, "Ready");
    }

    public static void waitForKafkaMirrorMaker2IsNotReady(String clusterName) {
        waitForKafkaMirrorMaker2Status(clusterName, "NotReady");
    }
}
