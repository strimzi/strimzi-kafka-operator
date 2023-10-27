/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;

public class JobResource implements ResourceType<Job> {

    @Override
    public String getKind() {
        return TestConstants.JOB;
    }
    @Override
    public Job get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getJob(name);
    }
    @Override
    public void create(Job resource) {
        ResourceManager.kubeClient().createJob(resource);
    }
    @Override
    public void delete(Job resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteJob(resource.getMetadata().getName());
    }

    @Override
    public void update(Job resource) {
        ResourceManager.kubeClient().updateJob(resource);
    }

    @Override
    public boolean waitForReadiness(Job resource) {
        return JobUtils.waitForJobRunning(resource.getMetadata().getName(), resource.getMetadata().getNamespace());
    }
}
