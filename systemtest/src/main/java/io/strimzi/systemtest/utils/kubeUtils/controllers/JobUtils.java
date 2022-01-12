/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
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
     * Wait until all Jobs are deleted in given namespace.
     * @param namespace Delete all jobs in this namespace
     */
    public static void removeAllJobs(String namespace) {
        kubeClient().namespace(namespace).getJobList().getItems().forEach(
            job -> JobUtils.deleteJobWithWait(namespace, job.getMetadata().getName()));
    }

    /**
     * Wait until the given Job has been deleted.
     * @param name The name of the Job
     */
    public static void waitForJobDeletion(final String namespaceName, String name) {
        LOGGER.debug("Waiting for ReplicaSet of Deployment {} deletion", name);
        TestUtils.waitFor("ReplicaSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> kubeClient(namespaceName).listPodNamesInSpecificNamespace(namespaceName, "job-name", name).isEmpty());
        LOGGER.debug("Job {} was deleted", name);
    }

    /**
     * Delete Job and wait for it's deletion
     * @param name name of the job
     * @param namespace name of the namespace
     */
    public static void deleteJobWithWait(String namespace, String name) {
        kubeClient(namespace).deleteJob(namespace, name);
        waitForJobDeletion(namespace, name);
    }

    /**
     * Wait for specific Job failure
     * @param jobName job name
     * @param timeout timeout in ms after which we assume that job failed
     */
    public static void waitForJobFailure(String jobName, String namespace, long timeout) {
        LOGGER.info("Waiting for job: {} will be in error state", jobName);
        TestUtils.waitFor("job finished", Constants.GLOBAL_POLL_INTERVAL, timeout,
            () -> kubeClient().checkFailedJobStatus(namespace, jobName, 1));
    }

    /**
     * Wait for specific Job Running active status
     * @param jobName job name
     * @param namespace namespace
     */
    public static boolean waitForJobRunning(String jobName, String namespace) {
        LOGGER.info("Waiting for job: {} will be in active state", jobName);
        TestUtils.waitFor("job active", Constants.GLOBAL_POLL_INTERVAL, ResourceOperation.getTimeoutForResourceReadiness(Constants.JOB),
            () -> {
                JobStatus jb = kubeClient().namespace(namespace).getJobStatus(jobName);
                return jb.getActive() > 0;
            });

        return true;
    }
}
