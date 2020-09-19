/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ReplicaSetUtils {

    private static final Logger LOGGER = LogManager.getLogger(ReplicaSetUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private ReplicaSetUtils() { }

    /**
     * Wait until the given ReplicaSet has been deleted.
     * @param name The name of the ReplicaSet
     */
    public static void waitForReplicaSetDeletion(String name) {
        LOGGER.debug("Waiting for ReplicaSet of Deployment {} deletion", name);
        TestUtils.waitFor("ReplicaSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                if (!kubeClient().replicaSetExists(name)) {
                    return true;
                } else {
                    String rsName = kubeClient().getReplicaSetNameByPrefix(name);
                    LOGGER.warn("ReplicaSet {} is not deleted yet! Triggering force delete by cmd client!", rsName);
                    cmdKubeClient().deleteByName("replicaset", rsName);
                    return false;
                }
            });
        LOGGER.debug("ReplicaSet of Deployment {} was deleted", name);
    }
}
