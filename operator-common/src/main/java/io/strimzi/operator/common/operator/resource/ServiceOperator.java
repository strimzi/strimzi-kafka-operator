/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import static io.strimzi.operator.common.Annotations.LOADBALANCER_ANNOTATION_IGNORELIST;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Operations for {@code Service}s.
 */
public class ServiceOperator extends AbstractNamespacedResourceOperator<KubernetesClient, Service, ServiceList, ServiceResource<Service>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ServiceOperator.class);
    private static final Pattern IGNORABLE_PATHS = Pattern.compile(
            "^(/metadata/managedFields" +
                    "|/metadata/creationTimestamp" +
                    "|/metadata/resourceVersion" +
                    "|/metadata/generation" +
                    "|/metadata/uid" +
                    "|/spec/sessionAffinity" +
                    "|/spec/clusterIP" +
                    "|/spec/clusterIPs" +
                    "|/spec/ipFamily" + // Legacy field from Kube 1.19 and earlier. We just ignore it, it is not configurable.
                    "|/spec/ipFamilies" + // Immutable field
                    "|/spec/internalTrafficPolicy" + // Set by Kubernetes to Cluster as default (not configurable through Strimzi as it does nto seem to make much sense for us, so we ignore it)
                    "|/status)$");

    private final EndpointOperator endpointOperations;
    /**
     * Constructor
     *
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ServiceOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Service");
        this.endpointOperations = new EndpointOperator(vertx, client);
    }

    @Override
    protected MixedOperation<Service, ServiceList, ServiceResource<Service>> operation() {
        return client.services();
    }

    /**
     * @return  Returns the Pattern for matching paths which can be ignored in the resource diff
     */
    protected Pattern ignorablePaths() {
        return IGNORABLE_PATHS;
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     *
     * ServiceOperator needs its own version of this method to patch the NodePorts for NodePort and LoadBalancer type Services.
     * Patching the service with service definition without the NodePort would cause regenerating the node port
     * which triggers rolling update.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the service
     * @param name      Name of the service
     * @param current   Current service
     * @param desired   Desired Service
     *
     * @return  Future with reconciliation result
     */
    @Override
    protected Future<ReconcileResult<Service>> internalPatch(Reconciliation reconciliation, String namespace, String name, Service current, Service desired) {
        try {
            if (current.getSpec() != null && desired.getSpec() != null) {
                if (("NodePort".equals(current.getSpec().getType()) && "NodePort".equals(desired.getSpec().getType()))
                        || ("LoadBalancer".equals(current.getSpec().getType()) && "LoadBalancer".equals(desired.getSpec().getType())))   {
                    patchNodePorts(current, desired);
                    patchHealthCheckPorts(current, desired);
                    patchAnnotations(current, desired);
                }

                patchDualStackNetworking(current, desired);
            }

            return super.internalPatch(reconciliation, namespace, name, current, desired);
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Finds annotations managed by the Rancher cattle agents (if any), and merge them into the desired spec.
     *
     * This makes sure there is no infinite loop where Rancher tries to add annotations, while Rancher keeps
     * removing them during reconciliation.
     *
     * @param current   Current Service
     * @param desired   Desired Service
     */
    private void patchAnnotations(Service current, Service desired) {
        Map<String, String> currentAnnotations = current.getMetadata().getAnnotations();
        if (currentAnnotations != null) {
            Map<String, String> matchedAnnotations = currentAnnotations.keySet().stream()
                    .filter(annotation -> LOADBALANCER_ANNOTATION_IGNORELIST.stream().anyMatch(ignorelist -> ignorelist.test(annotation)))
                    .collect(Collectors.toMap(Function.identity(), currentAnnotations::get));

            if (!matchedAnnotations.isEmpty()) {
                desired.getMetadata().setAnnotations(Util.mergeLabelsOrAnnotations(
                        desired.getMetadata().getAnnotations(),
                        matchedAnnotations
                ));
            }
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

    /**
     * When a dedicated health check port is used by the current service, this method will patch it in the desired
     * service to avoid the health check port changing with every reconciliation. Similarly to the generated node ports,
     * the health check port is set by Kubernetes in the spec section and needs to be manually reconciled to avoid issues.
     *
     * @param current   Current Service
     * @param desired   Desired Service
     */
    protected void patchHealthCheckPorts(Service current, Service desired) {
        if (current.getSpec().getHealthCheckNodePort() != null
                && desired.getSpec().getHealthCheckNodePort() == null) {
            desired.getSpec().setHealthCheckNodePort(current.getSpec().getHealthCheckNodePort());
        }
    }

    /**
     * DualStack Kubernetes clusters (https://kubernetes.io/docs/concepts/services-networking/dual-stack/) specify
     * ipFamilyPolicy and ipFamilies fields of the Service spec section. This fields indicates whether IPv4 or IPv6
     * should be used. Kubernetes have different defaults for these fields depending on the environment. So when they
     * are not set by us / Strimzi user, we just copy the default set by Kubernetes. In addition, the ipFamilies field
     * is immutable and cannot be changed. This method is used to patch the service and set the value from the current
     * resource in the desired to allow to patch / reconcile the Service. Without this the reconciliation would fail.
     *
     * @param current   Current Service
     * @param desired   Desired Service
     */
    protected void patchDualStackNetworking(Service current, Service desired) {
        desired.getSpec().setIpFamilies(current.getSpec().getIpFamilies());

        if (desired.getSpec().getIpFamilyPolicy() == null) {
            desired.getSpec().setIpFamilyPolicy(current.getSpec().getIpFamilyPolicy());
        }
    }

    /**
     * Deletes the resource with the given namespace and name and completes the given future accordingly.
     * This method will do a cascading delete.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     *
     * @return Future with result of the reconciliation
     */
    protected Future<ReconcileResult<Service>> internalDelete(Reconciliation reconciliation, String namespace, String name) {
        return internalDelete(reconciliation, namespace, name, true);
    }

    /**
     * Waits for endpoint readiness
     *
     * @param reconciliation        Reconciliation marker
     * @param namespace             Namespace
     * @param name                  Name
     * @param pollInterval          Interval in which it will poll for the readiness
     * @param operationTimeoutMs    How long to wait for the endpoint to be ready
     *
     * @return  Future which completes when the endpoint is ready
     */
    public Future<Void> endpointReadiness(Reconciliation reconciliation, String namespace, String name, long pollInterval, long operationTimeoutMs) {
        return endpointOperations.readiness(reconciliation, namespace, name, pollInterval, operationTimeoutMs);
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
    public Future<Void> hasNodePort(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
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
