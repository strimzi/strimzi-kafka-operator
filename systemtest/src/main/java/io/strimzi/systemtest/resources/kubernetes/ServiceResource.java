/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ServiceResource implements ResourceType<Service> {

    private static final Logger LOGGER = LogManager.getLogger(ServiceResource.class);

    @Override
    public String getKind() {
        return Constants.SERVICE;
    }
    @Override
    public Service get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getService(name);
    }
    @Override
    public void create(Service resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createService(resource);
    }
    @Override
    public void delete(Service resource) throws Exception {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteService(resource);
    }
    @Override
    public boolean waitForReadiness(Service resource) {
        return resource != null;
    }

    public static Service createServiceResource(ExtensionContext extensionContext, Service service, String clientNamespace) {
        LOGGER.info("Creating Service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        ResourceManager.getInstance().createResource(extensionContext, service);
        return service;
    }
}
