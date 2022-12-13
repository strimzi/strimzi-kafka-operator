/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Endpoint}s.
 */
public class EndpointOperator extends AbstractReadyNamespacedResourceOperator<KubernetesClient, Endpoints, EndpointsList, Resource<Endpoints>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    EndpointOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Endpoints");
    }

    @Override
    protected MixedOperation<Endpoints, EndpointsList, Resource<Endpoints>> operation() {
        return client.endpoints();
    }
}
