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

/**
 * Operations for {@code Endpoint}s.
 */
public class EndpointOperator extends AbstractReadyNamespacedResourceOperator<KubernetesClient, Endpoints, EndpointsList, Resource<Endpoints>> {
    /**
     * Constructor
     * @param client The Kubernetes client
     */
    EndpointOperator(KubernetesClient client) {
        super(client, "Endpoints");
    }

    @Override
    protected MixedOperation<Endpoints, EndpointsList, Resource<Endpoints>> operation() {
        return client.endpoints();
    }
}
