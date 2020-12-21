package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class DeploymentResource implements ResourceType<Deployment> {

    @Override
    public String getKind() {
        return "Deployment";
    }
    @Override
    public Deployment get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getDeployment(name);
    }
    @Override
    public void create(Deployment resource) {
        ResourceManager.kubeClient().createOrReplaceDeployment(resource);
    }
    @Override
    public void delete(Deployment resource) throws Exception {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteDeployment(resource.getMetadata().getName());
    }

    @Override
    public boolean isReady(Deployment resource) {
        return DeploymentUtils.waitForDeploymentReady(resource.getMetadata().getName());

    }
    @Override
    public void refreshResource(Deployment existing, Deployment newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }

    public static DoneableDeployment deployNewDeployment(Deployment deployment) {
        return new DoneableDeployment(deployment, co -> {
            TestUtils.waitFor("Deployment creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
                () -> {
                    try {
                        ResourceManager.kubeClient().createOrReplaceDeployment(co);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return co;
        });
    }

    public static Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }
}
