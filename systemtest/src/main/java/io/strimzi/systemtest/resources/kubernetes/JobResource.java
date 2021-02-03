/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class JobResource implements ResourceType<Job> {

    @Override
    public String getKind() {
        return "Job";
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
    public void delete(Job resource) throws Exception {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteJob(resource.getMetadata().getName());
    }
    @Override
    public boolean isReady(Job resource) {
        return resource != null;
    }
    @Override
    public void refreshResource(Job existing, Job newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }

    public static Job deployNewJob(Job job) {
        TestUtils.waitFor("Job creation " + job.getMetadata().getName(), Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
            () -> {
                try {
                    ResourceManager.kubeClient().createJob(job);
                    return true;
                } catch (KubernetesClientException e) {
                    if (e.getMessage().contains("object is being deleted")) {
                        return false;
                    } else {
                        throw e;
                    }
                }
            });
        return job;
    }
}
