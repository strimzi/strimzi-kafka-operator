/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;

public class ServiceResource implements ResourceType<Service> {

    private static final Logger LOGGER = LogManager.getLogger(ServiceResource.class);

    @Override
    public String getKind() {
        return "Service";
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
    public boolean isReady(Service resource) {
        return resource != null;
    }
    @Override
    public void refreshResource(Service existing, Service newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }

    public static ServiceBuilder getSystemtestsServiceResource(String appName, int port, String namespace, String transportProtocol) {
        return new ServiceBuilder()
            .withNewMetadata()
                .withName(appName)
                .withNamespace(namespace)
                .addToLabels("run", appName)
            .endMetadata()
            .withNewSpec()
                .withSelector(Collections.singletonMap("app", appName))
                .addNewPort()
                    .withName("http")
                    .withPort(port)
                    .withProtocol(transportProtocol)
                .endPort()
            .endSpec();
    }

    public static Service createServiceResource(ExtensionContext extensionContext, String appName, int port, String clientNamespace, String transportProtocol) {
        Service service = getSystemtestsServiceResource(appName, port, clientNamespace, transportProtocol).build();
        LOGGER.info("Creating Service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        ResourceManager.getInstance().createResource(extensionContext, service);
        return service;
    }

    public static Service createServiceResource(ExtensionContext extensionContext, Service service, String clientNamespace) {
        LOGGER.info("Creating Service {} in namespace {}", service.getMetadata().getName(), clientNamespace);
        ResourceManager.getInstance().createResource(extensionContext, service);
        return service;
    }
}
