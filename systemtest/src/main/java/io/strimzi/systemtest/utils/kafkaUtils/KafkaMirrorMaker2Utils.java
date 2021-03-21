/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaMirrorMaker2Utils {

    private KafkaMirrorMaker2Utils() {}

    /**
     * Wait until KafkaMirrorMaker2 will be in desired state
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static boolean waitForKafkaMirrorMaker2Status(String clusterName, Enum<?>  state) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client(), kafkaMirrorMaker2, state);
    }

    public static boolean waitForKafkaMirrorMaker2Ready(String clusterName) {
        return waitForKafkaMirrorMaker2Status(clusterName, Ready);
    }

    public static boolean waitForKafkaMirrorMaker2NotReady(String clusterName) {
        return waitForKafkaMirrorMaker2Status(clusterName, NotReady);
    }
}
