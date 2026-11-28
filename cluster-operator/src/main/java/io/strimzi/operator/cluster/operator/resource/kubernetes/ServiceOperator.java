/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Operations for {@code Service}s.
 */
public class ServiceOperator extends AbstractNamespacedResourceOperator<KubernetesClient, Service, ServiceList, ServiceResource<Service>> {
    /**
     * Constructor
     *
     * @param asyncExecutor         Executor to use for asynchronous subroutines
     * @param client                The Kubernetes client
     */
    public ServiceOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "Service", true);
    }

    @Override
    protected MixedOperation<Service, ServiceList, ServiceResource<Service>> operation() {
        return client.services();
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
     * Checks if the Service already has assigned ingress address.
     *
     * @param namespace The namespace.
     * @param name The route name.
     * @return Whether the Service already has assigned ingress address.
     */
    public boolean isIngressAddressReady(String namespace, String name) {
        ServiceResource<Service> resourceOp = operation().inNamespace(namespace).withName(name);
        Service resource = resourceOp.get();

        if (resource != null && resource.getStatus() != null && resource.getStatus().getLoadBalancer() != null && resource.getStatus().getLoadBalancer().getIngress() != null && resource.getStatus().getLoadBalancer().getIngress().size() > 0) {
            if (resource.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null || resource.getStatus().getLoadBalancer().getIngress().get(0).getIp() != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Succeeds when the Service has an assigned node port
     *
     * @param reconciliation The reconciliation
     * @param namespace     Namespace
     * @param name          Name of the service
     * @param pollIntervalMs    Interval in which we poll
     * @param timeoutMs     Timeout
     * @return A future that succeeds when the Service has an assigned node port
     */
    public CompletionStage<Void> hasNodePort(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, pollIntervalMs, timeoutMs, this::isNodePortReady);
    }

    /**
     * Checks if the Service already has assigned node ports.
     *
     * @param namespace The namespace.
     * @param name The route name.
     * @return Whether the Service already has assigned node ports.
     */
    public boolean isNodePortReady(String namespace, String name) {
        ServiceResource<Service> resourceOp = operation().inNamespace(namespace).withName(name);
        Service resource = resourceOp.get();

        if (resource != null && resource.getSpec() != null && resource.getSpec().getPorts() != null) {
            boolean ready = true;

            for (ServicePort port : resource.getSpec().getPorts())  {
                if (port.getNodePort() == null) {
                    ready = false;
                }
            }
            return ready;
        }

        return false;
    }
}
