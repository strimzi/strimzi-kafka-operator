package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.apps.Deployment;
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
        return DeploymentUtils.waitForDeploymentAndPodsReady(resource.getMetadata().getName(), resource.getSpec().getReplicas());

    }
    @Override
    public void refreshResource(Deployment existing, Deployment newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }

    public static Deployment deployNewDeployment(Deployment deployment) {
        TestUtils.waitFor("Deployment creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
            () -> {
                try {
                    ResourceManager.kubeClient().createOrReplaceDeployment(deployment);
                    return true;
                } catch (KubernetesClientException e) {
                    if (e.getMessage().contains("object is being deleted")) {
                        return false;
                    } else {
                        throw e;
                    }
                }
            });
        return deployment;
    }

    public static Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }
}
