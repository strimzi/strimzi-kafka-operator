/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.draincleaner;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.resources.kubernetes.DeploymentResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;

public class DrainCleanerResource implements ResourceType<Deployment> {
    public static final String PATH_TO_DRAIN_CLEANER_DEP = TestUtils.USER_PATH + "/../packaging/install/drain-cleaner/kubernetes/060-Deployment.yaml";

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }

    @Override
    public Deployment get(String namespace, String name) {
        String deploymentName = ResourceManager.kubeClient().namespace(namespace).getDeploymentNameByPrefix(name);
        return deploymentName != null ? ResourceManager.kubeClient().getDeployment(namespace, deploymentName) : null;
    }

    @Override
    public void create(Deployment resource) {
        ResourceManager.kubeClient().createOrReplaceDeployment(resource);
    }

    @Override
    public void delete(Deployment resource) {
        ResourceManager.kubeClient().deleteDeployment(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public boolean waitForReadiness(Deployment resource) {
        return resource != null
            && resource.getMetadata() != null
            && resource.getMetadata().getName() != null
            && resource.getStatus() != null
            && DeploymentUtils.waitForDeploymentAndPodsReady(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), resource.getSpec().getReplicas());
    }

    public DeploymentBuilder buildDrainCleanerDeployment() {
        Deployment drainCleaner = DeploymentResource.getDeploymentFromYaml(PATH_TO_DRAIN_CLEANER_DEP);

        return new DeploymentBuilder(drainCleaner)
            .editOrNewMetadata()
                .addToLabels(Constants.DEPLOYMENT_TYPE, DeploymentTypes.DrainCleaner.name())
            .endMetadata();
    }
}
