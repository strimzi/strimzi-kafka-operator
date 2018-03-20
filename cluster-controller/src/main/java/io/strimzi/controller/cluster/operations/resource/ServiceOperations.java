/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Service}s.
 */
public class ServiceOperations extends AbstractOperations<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> {

    private final EndpointOperations endpointOperations;
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ServiceOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Service");
        this.endpointOperations = new EndpointOperations(vertx, client);
    }

    @Override
    protected MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> operation() {
        return client.services();
    }

    public Future<Void> endpointReadiness(String namespace, Service desired, long pollInterval, long operationTimeoutMs) {
        return endpointOperations.readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs);
    }
}
