/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class JobUtils {

    private static final Logger LOGGER = LogManager.getLogger(JobUtils.class);

    private JobUtils() { }

    /**
     * Checks if {@link Job} running in specified Namespace succeeded or not - based on number
     * of successful Pods.
     *
     * @param namespaceName             Namespace name where the Job should be present.
     * @param jobName                   Name of the Job for which the status should be checked.
     * @param expectedSucceededPods     Expected number of succeeded Pods
     * @return  if {@link Job} running in specified Namespace succeeded or not
     */
    public static boolean checkSucceededJobStatus(String namespaceName, String jobName, int expectedSucceededPods) {
        Job job = KubeResourceManager.get().kubeClient().getClient().batch().v1().jobs().inNamespace(namespaceName).withName(jobName).get();
        JobStatus jobStatus = job == null ? null : job.getStatus();
        return jobStatus != null && jobStatus.getSucceeded() != null && jobStatus.getSucceeded().equals(expectedSucceededPods);
    }

    /**
     * Checks if {@link Job} running in specified Namespace failed or not - based on number
     * of failed Pods.
     *
     * @param namespaceName         Namespace name where the Job should be present.
     * @param jobName               Name of the Job for which the status should be checked.
     * @param expectedFailedPods    Expected number of failed Pods
     * @return  if {@link Job} running in specified Namespace failed or not
     */
    public static boolean checkFailedJobStatus(String namespaceName, String jobName, int expectedFailedPods) {
        Job job = KubeResourceManager.get().kubeClient().getClient().batch().v1().jobs().inNamespace(namespaceName).withName(jobName).get();
        JobStatus jobStatus = job == null ? null : job.getStatus();
        return jobStatus != null && jobStatus.getFailed() != null && jobStatus.getFailed().equals(expectedFailedPods);
    }

    /**
     * Wait until the Pod of Job with {@param jobName} contains specified {@param logMessage}
     * @param namespaceName name of Namespace where the Pod is running
     * @param jobName name of Job with which the Pod name obtained
     * @param logMessage desired log message
     */
    public static void waitForJobContainingLogMessage(String namespaceName, String jobName, String logMessage) {
        String jobPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespaceName, jobName).get(0).getMetadata().getName();

        TestUtils.waitFor("Job contains log message: " + logMessage, TestConstants.GLOBAL_POLL_INTERVAL_LONG, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getLogsFromPod(namespaceName, jobPodName).contains(logMessage));
    }

    /**
     * Delete Job and wait for it's deletion
     * @param namespaceName     name of the Namespace
     * @param jobName           name of the job
     */
    public static void deleteJobWithWait(String namespaceName, String jobName) {
        Job job = KubeResourceManager.get().kubeClient().getClient().batch().v1().jobs().inNamespace(namespaceName).withName(jobName).get();

        if (job != null) {
            KubeResourceManager.get().deleteResourceWithWait(job);
        }
    }

    /**
     * Delete Jobs with wait
     * @param namespace - name of the namespace
     * @param names - job names
     */
    public static void deleteJobsWithWait(String namespace, String... names) {
        for (String jobName : names) {
            deleteJobWithWait(namespace, jobName);
            // wait for all Pods for the Job to be deleted before continuing
            PodUtils.waitForPodsWithPrefixDeletion(namespace, jobName);
        }
    }

    /**
     * Wait for specific Job success
     * @param jobName job name
     * @param timeout timeout in ms after which we assume that job failed
     */
    public static void waitForJobSuccess(String namespaceName, String jobName, long timeout) {
        LOGGER.info("Waiting for Job: {}/{} to success", namespaceName, jobName);
        TestUtils.waitFor("success of Job: " + namespaceName + "/" + jobName, TestConstants.GLOBAL_POLL_INTERVAL, timeout,
                () -> JobUtils.checkSucceededJobStatus(namespaceName, jobName, 1));
    }

    /**
     * Wait for specific Job failure
     * @param jobName job name
     * @param timeout timeout in ms after which we assume that job failed
     */
    public static void waitForJobFailure(String namespaceName, String jobName, long timeout) {
        LOGGER.info("Waiting for Job: {}/{} to fail", namespaceName, jobName);
        TestUtils.waitFor("failure of Job: " + namespaceName + "/" + jobName, TestConstants.GLOBAL_POLL_INTERVAL, timeout,
            () -> JobUtils.checkFailedJobStatus(namespaceName, jobName, 1));
    }

    /**
     * Log actual status of Job with pods.
     *
     * @param namespaceName - namespace/project where is job running
     * @param jobName       - name of the job, for which we should scrape status
     */
    public static void logCurrentJobStatus(String namespaceName, String jobName) {
        Job currentJob = KubeResourceManager.get().kubeClient().getClient().batch().v1().jobs().inNamespace(namespaceName).withName(jobName).get();

        if (currentJob != null && currentJob.getStatus() != null) {
            List<String> log = new ArrayList<>(asList(TestConstants.JOB, " status:\n"));

            List<JobCondition> conditions = currentJob.getStatus().getConditions();

            log.add("\tActive: " + currentJob.getStatus().getActive());
            log.add("\n\tFailed: " + currentJob.getStatus().getFailed());
            log.add("\n\tReady: " + currentJob.getStatus().getReady());
            log.add("\n\tSucceeded: " + currentJob.getStatus().getSucceeded());

            if (conditions != null) {
                List<String> conditionList = new ArrayList<>();

                for (JobCondition condition : conditions) {
                    if (condition.getMessage() != null) {
                        conditionList.add("\t\tType: " + condition.getType() + "\n");
                        conditionList.add("\t\tMessage: " + condition.getMessage() + "\n");
                    }
                }

                if (!conditionList.isEmpty()) {
                    log.add("\n\tConditions:\n");
                    log.addAll(conditionList);
                }
            }

            log.add("\n\nPods with conditions and messages:\n\n");

            for (Pod pod : KubeResourceManager.get().kubeClient().listPodsByPrefixInName(currentJob.getMetadata().getNamespace(), jobName)) {
                log.add(pod.getMetadata().getName() + ":");
                List<String> podConditions = new ArrayList<>();

                for (PodCondition podCondition : pod.getStatus().getConditions()) {
                    if (podCondition.getMessage() != null) {
                        podConditions.add("\n\tType: " + podCondition.getType() + "\n");
                        podConditions.add("\tMessage: " + podCondition.getMessage() + "\n");
                    }
                }

                if (podConditions.isEmpty()) {
                    log.add("\n\t<EMPTY>");
                } else {
                    log.addAll(podConditions);
                }
                log.add("\n\n");
            }
            LOGGER.info("{}", String.join("", log).strip());
        }
    }
}
