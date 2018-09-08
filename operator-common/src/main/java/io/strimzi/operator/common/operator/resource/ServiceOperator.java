/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Service}s.
 */
public class ServiceOperator extends AbstractResourceOperator<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> {

    private final EndpointOperator endpointOperations;
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ServiceOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Service");
        this.endpointOperations = new EndpointOperator(vertx, client);
    }

    @Override
    protected MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> operation() {
        return client.services();
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     *
     * ServiceOperator needs its own version of this method to patch the NodePorts for NodePort type Services.
     * Patching NodePort service with service definition without the NodePort would cause regenerating the node port
     * which triggers rolling update.
     *
     * @param namespace Namespace of the service
     * @param name      Name of the service
     * @param current   Current servicve
     * @param desired   Desired Service
     *
     * @return  Future with reconciliation result
     */
    protected Future<ReconcileResult<Service>> internalPatch(String namespace, String name, Service current, Service desired) {
        try {
            if (current.getSpec() != null && "NodePort".equals(current.getSpec().getType())
                    && desired.getSpec() != null && "NodePort".equals(desired.getSpec().getType()))   {
                patchNodePorts(current, desired);
            }

            ReconcileResult.Patched<Service> result = ReconcileResult.patched(operation().inNamespace(namespace).withName(name).cascading(true).patch(desired));
            log.debug("{} {} in namespace {} has been patched", resourceKind, name, namespace);
            return Future.succeededFuture(result);
        } catch (Exception e) {
            log.error("Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Finds out if corresponding port from desired service also exists in current service.
     * If it exists, it will copy the node port.
     * That will make sure the node port doesn't change with every reconciliation.
     *
     * @param current   Current Service
     * @param desired   Desired Service
     */
    protected void patchNodePorts(Service current, Service desired) {
        for (ServicePort desiredPort : desired.getSpec().getPorts())    {
            String portName = desiredPort.getName();

            for (ServicePort currentPort : current.getSpec().getPorts())    {
                if (desiredPort.getNodePort() == null && portName.equals(currentPort.getName()) && currentPort.getNodePort() != null) {
                    desiredPort.setNodePort(currentPort.getNodePort());
                }
            }
        }
    }

    public Future<Void> endpointReadiness(String namespace, Service desired, long pollInterval, long operationTimeoutMs) {
        return endpointOperations.readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs);
    }

    /**
     * Succeeds when the Service has an assigned address
     *
     * @param namespace     Namespace
     * @param name          Name of the service
     * @param pollIntervalMs    Interval in which we poll
     * @param timeoutMs     Timeout
     * @return
     */
    public Future<Void> hasIngressAddress(String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(namespace, name, pollIntervalMs, timeoutMs, this::isIngressAddressReady);
    }

    /**
     * Checks if the Service already has assigned ingress address.
     *
     * @param namespace The namespace.
     * @param name The route name.
     */
    public boolean isIngressAddressReady(String namespace, String name) {
        Resource<Service, DoneableService> resourceOp = operation().inNamespace(namespace).withName(name);
        Service resource = resourceOp.get();

        if (resource != null && resource.getStatus() != null && resource.getStatus().getLoadBalancer() != null && resource.getStatus().getLoadBalancer().getIngress() != null && resource.getStatus().getLoadBalancer().getIngress().get(0) != null) {
            if (resource.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null || resource.getStatus().getLoadBalancer().getIngress().get(0).getIp() != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Succeeds when the Service has an assigned node port
     *
     * @param namespace     Namespace
     * @param name          Name of the service
     * @param pollIntervalMs    Interval in which we poll
     * @param timeoutMs     Timeout
     * @return
     */
    public Future<Void> hasNodePort(String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(namespace, name, pollIntervalMs, timeoutMs, this::isNodePortReady);
    }

    /**
     * Checks if the Service already has assigned node ports.
     *
     * @param namespace The namespace.
     * @param name The route name.
     */
    public boolean isNodePortReady(String namespace, String name) {
        Resource<Service, DoneableService> resourceOp = operation().inNamespace(namespace).withName(name);
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
