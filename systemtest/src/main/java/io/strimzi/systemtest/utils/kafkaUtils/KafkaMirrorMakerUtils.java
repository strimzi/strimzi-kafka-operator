/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaMirrorMakerUtils {

    private KafkaMirrorMakerUtils() {}

    /**
     * Wait until KafkaMirrorMaker status is in desired state
     * @param namespace Namespace where MirrorMaker resource is located
     * @param clusterName name of KafkaMirrorMaker cluster
     * @param state desired state - like Ready
     */
    // Deprecation is suppressed because of KafkaMirrorMaker
    @SuppressWarnings("deprecation")
    public static boolean waitForKafkaMirrorMakerStatus(String namespace, String clusterName, Enum<?>  state) {
        KafkaMirrorMaker kafkaMirrorMaker = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespace).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaMirrorMakerResource.kafkaMirrorMakerClient(), kafkaMirrorMaker, state);
    }

    public static boolean waitForKafkaMirrorMakerReady(String namespace, String clusterName) {
        return waitForKafkaMirrorMakerStatus(namespace, clusterName, Ready);
    }

    public static boolean waitForKafkaMirrorMakerNotReady(final String namespace, String clusterName) {
        return waitForKafkaMirrorMakerStatus(namespace, clusterName, NotReady);
    }
}
