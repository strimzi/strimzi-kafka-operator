/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.ResourceManagerPhase;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaClientsResource implements ResourceType<Deployment> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientsResource.class);

    public KafkaClientsResource() {}

    @Override
    public String getKind() {
        return "Deployment";
    }
    @Override
    public Deployment get(String namespace, String name) {
        String deploymentName = ResourceManager.kubeClient().namespace(namespace).getDeploymentNameByPrefix(name);
        return deploymentName != null ?  ResourceManager.kubeClient().getDeployment(deploymentName) : null;
//        Deployment[] deployment = new Deployment[1];

//        switch (phase) {
//            case CREATE_PHASE:
//                TestUtils.waitFor(" until deployment is present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
//                    () -> {
//                        deployment[0] = ResourceManager.kubeClient().namespace(namespace).getDeployment(ResourceManager.kubeClient().getDeploymentNameByPrefix(name));
//                        return deployment[0] != null;
//                    });
//                return ResourceManager.kubeClient().namespace(namespace).getDeployment(ResourceManager.kubeClient().getDeploymentNameByPrefix(name));
//            case DELETE_PHASE:
//                TestUtils.waitFor(" until deployment is null", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
//                    () -> {
//                        try {
//                            deployment[0] = ResourceManager.kubeClient().namespace(namespace).getDeployment(ResourceManager.kubeClient().getDeploymentNameByPrefix(name));
//                            return deployment[0] == null;
//                        } catch (IndexOutOfBoundsException | NullPointerException exception) {
//                            // object is null so we can return true
//                            return true;
//                        }
//                    });
//                // after successful dynamic wait  we can return null
//                return null;
//            default:
//                return null;
//        }
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
        resource = ClientUtils.waitUntilClientsArePresent(resource);

        return DeploymentUtils.waitForDeploymentReady(resource.getMetadata().getName());
    }

    @Override
    public void refreshResource(Deployment existing, Deployment newResource) {
        existing = ClientUtils.waitUntilClientsArePresent(existing);

        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }
}