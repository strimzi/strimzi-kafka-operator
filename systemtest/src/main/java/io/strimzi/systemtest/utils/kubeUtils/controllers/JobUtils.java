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

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class JobUtils {

    private static final Logger LOGGER = LogManager.getLogger(JobUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private JobUtils() { }

    /**
     * Wait until the given Job has been deleted.
     * @param name The name of the Job
     */
    public static void waitForJobDeletion(String name) {
        LOGGER.debug("Waiting for ReplicaSet of Deployment {} deletion", name);
        TestUtils.waitFor("ReplicaSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> kubeClient().listPods("job-name", name).isEmpty());
        LOGGER.debug("Job {} was deleted", name);
    }

    /**
     * Delete Job and wait for it's deletion
     * @param namespace namespace where job is deployed
     * @param name name of the job
     */
    public static void deleteJob(String namespace, String name) {
        kubeClient().getClient().batch().jobs().inNamespace(namespace).withName(name).delete();
        waitForJobDeletion(name);
    }
}
