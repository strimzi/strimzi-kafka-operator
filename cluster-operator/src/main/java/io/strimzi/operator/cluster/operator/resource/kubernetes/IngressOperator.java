/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Operations for {@code Ingress}es.
 */
public class IngressOperator extends AbstractNamespacedResourceOperator<KubernetesClient, Ingress, IngressList, Resource<Ingress>> {
    /**
     * Constructor
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client
     */
    public IngressOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "Ingress", true);
    }

    @Override
    protected MixedOperation<Ingress, IngressList, Resource<Ingress>> operation() {
        return client.network().v1().ingresses();
    }

    /**
     * Succeeds when the Service has an assigned address
     *
     * @param reconciliation The reconciliation
     * @param namespace     Namespace
     * @param name          Name of the service
     * @param pollIntervalMs    Interval in which we poll
     * @param timeoutMs     Timeout
     * @return A future that succeeds when the Service has an assigned address.
     */
    public CompletionStage<Void> hasIngressAddress(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, "addressable", pollIntervalMs, timeoutMs, this::isIngressAddressReady);
    }

    /**
     * Checks if the Ingress already has assigned ingress address.
     *
     * @param namespace The namespace.
     * @param name The route name.
     * @return Whether the Ingress already has assigned ingress address.
     */
    public boolean isIngressAddressReady(String namespace, String name) {
        Resource<Ingress> resourceOp = operation().inNamespace(namespace).withName(name);
        Ingress resource = resourceOp.get();

        if (resource != null && resource.getStatus() != null && resource.getStatus().getLoadBalancer() != null && resource.getStatus().getLoadBalancer().getIngress() != null && resource.getStatus().getLoadBalancer().getIngress().size() > 0) {
            if (resource.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null || resource.getStatus().getLoadBalancer().getIngress().get(0).getIp() != null) {
                return true;
            }
        }

        return false;
    }
}
