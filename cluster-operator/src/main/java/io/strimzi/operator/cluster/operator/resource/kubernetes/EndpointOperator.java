/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractReadyNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operations for {@code Endpoint}s.
 */
public class EndpointOperator extends AbstractReadyNamespacedResourceOperator<KubernetesClient, Endpoints, EndpointsList, Resource<Endpoints>> {
    /**
     * Constructor
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client
     */
    EndpointOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "Endpoints");
    }

    @Override
    protected MixedOperation<Endpoints, EndpointsList, Resource<Endpoints>> operation() {
        return client.endpoints();
    }
}
