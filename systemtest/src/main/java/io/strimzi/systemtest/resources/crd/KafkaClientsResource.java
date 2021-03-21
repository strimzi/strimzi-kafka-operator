/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaClientsResource implements ResourceType<Deployment> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientsResource.class);

    public KafkaClientsResource() {}

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }
    @Override
    public Deployment get(String namespace, String name) {
        String deploymentName = ResourceManager.kubeClient().namespace(namespace).getDeploymentNameByPrefix(name);
        return deploymentName != null ?  ResourceManager.kubeClient().getDeployment(deploymentName) : null;
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
    public boolean waitForReadiness(Deployment resource) {
        resource = ClientUtils.waitUntilClientsArePresent(resource);

        return DeploymentUtils.waitForDeploymentReady(resource.getMetadata().getName());
    }
}