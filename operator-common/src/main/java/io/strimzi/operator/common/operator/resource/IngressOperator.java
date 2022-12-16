/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Ingress}es.
 */
public class IngressOperator extends AbstractNamespacedResourceOperator<KubernetesClient, Ingress, IngressList, Resource<Ingress>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public IngressOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Ingress");
    }

    @Override
    protected MixedOperation<Ingress, IngressList, Resource<Ingress>> operation() {
        return client.network().v1().ingresses();
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     *
     * IngressOperator needs its own version of this method to patch the Ingress Class name when the default class is
     * used. When the default IngressClass is used, the class name in the Kafka CR should not be set at all and the
     * Ingress resources will be created without it. The default Kubernetes Nginx Ingress will then pick up the Ingress
     * resource and inject the class name into it. So when patching the Ingress resource, we need to set the IngressClass
     * name in the desired object to match the current. This needs to be done in order to:
     *     - Avoid unnecessary patching
     *     - Because if you change the class, the Kubernetes Nginx Ingress controller will not pick it up again, and it
     *       will stop working
     *
     * This is done only when the IngressClass set in Kafka CR is null. If it is set to some specific class, it will be
     * of course kept.
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace of the Ingress resource
     * @param name              Name of the Ingress resource
     * @param current           Current Ingress resource
     * @param desired           Desired Ingress resource
     *
     * @return  Future with reconciliation result
     */
    @Override
    protected Future<ReconcileResult<Ingress>> internalPatch(Reconciliation reconciliation, String namespace, String name, Ingress current, Ingress desired) {
        patchIngressClassName(current, desired);

        return super.internalPatch(reconciliation, namespace, name, current, desired);
    }

    /**
     * Patches the IngressClass name from the current to the desired Ingress resource. The patching is only done if the
     * desired IngressClass name is not set.
     *
     * @param current           Current Ingress resource
     * @param desired           Desired Ingress resource
     */
    /* test */ void patchIngressClassName(Ingress current, Ingress desired) {
        if (desired.getSpec() != null
                && current.getSpec() != null
                && desired.getSpec().getIngressClassName() == null)   {
            desired.getSpec().setIngressClassName(current.getSpec().getIngressClassName());
        }
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
    public Future<Void> hasIngressAddress(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
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
