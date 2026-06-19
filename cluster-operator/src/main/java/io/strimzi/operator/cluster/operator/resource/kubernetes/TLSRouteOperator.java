/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.gatewayapi.v1.TLSRoute;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.TLSRouteList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Operations for {@code TLSRoute}s.
 */
public class TLSRouteOperator extends AbstractNamespacedResourceOperator<KubernetesClient, TLSRoute, TLSRouteList, Resource<TLSRoute>> {
    /**
     * Constructor
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client    The Kubernetes client
     */
    public TLSRouteOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "TLSRoute");
    }

    @Override
    protected MixedOperation<TLSRoute, TLSRouteList, Resource<TLSRoute>> operation() {
        return client.resources(TLSRoute.class, TLSRouteList.class);
    }

    /**
     * Succeeds when the TLSRoute has at least one assigned parent
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace
     * @param name              Name of the TLSRoute
     * @param pollIntervalMs    Interval in which we poll
     * @param timeoutMs         Timeout
     *
     * @return A future that succeeds when the TLSRoute has at least one parent.
     */
    public CompletionStage<Void> hasParents(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, "addressable", pollIntervalMs, timeoutMs, this::hasParents);
    }

    /**
     * Checks if the TLSRoute already has at least one parent.
     *
     * @param namespace     The namespace.
     * @param name          The TLSRoute name.
     *
     * @return Whether the TLSRoute is ready.
     */
    private boolean hasParents(String namespace, String name) {
        TLSRoute resource = get(namespace, name);

        return resource != null
                && resource.getStatus() != null
                && resource.getStatus().getParents() != null
                && !resource.getStatus().getParents().isEmpty();
    }
}
