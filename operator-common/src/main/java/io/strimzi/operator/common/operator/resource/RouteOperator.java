/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.StrimziFuture;

/**
 * Operations for {@code Route}s.
 */
public class RouteOperator extends AbstractNamespacedResourceOperator<OpenShiftClient, Route, RouteList, Resource<Route>> {
    /**
     * Constructor
     * @param client The OpenShift client
     */
    public RouteOperator(OpenShiftClient client) {
        super(client, "Route");
    }

    @Override
    protected MixedOperation<Route, RouteList, Resource<Route>> operation() {
        return client.routes();
    }

    /**
     * Succeeds when the Route has an assigned address.
     *
     * @param reconciliation The reconciliation
     * @param namespace     Namespace.
     * @param name          Name of the route.
     * @param pollIntervalMs    Interval in which we poll.
     * @param timeoutMs     Timeout.
     * @return A future that succeeds when the Route has an assigned address.
     */
    public StrimziFuture<Void> hasAddress(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, "addressable", pollIntervalMs, timeoutMs, this::isAddressReady);
    }

    /**
     * Checks if the Route already has an assigned address.
     *
     * @param namespace The namespace.
     * @param name The route name.
     * @return Whether the address is ready.
     */
    public boolean isAddressReady(String namespace, String name) {
        Resource<Route> resourceOp = operation().inNamespace(namespace).withName(name);
        Route resource = resourceOp.get();

        if (resource != null && resource.getStatus() != null && resource.getStatus().getIngress() != null && resource.getStatus().getIngress().size() > 0) {
            if (resource.getStatus().getIngress().get(0).getHost() != null) {
                return true;
            }
        }

        return false;
    }
}
