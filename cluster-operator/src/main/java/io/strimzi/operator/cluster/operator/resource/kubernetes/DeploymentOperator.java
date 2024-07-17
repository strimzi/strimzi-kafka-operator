/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Deployment}s.
 */
public class DeploymentOperator extends AbstractScalableNamespacedResourceOperator<KubernetesClient, Deployment, DeploymentList, RollableScalableResource<Deployment>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(DeploymentOperator.class);

    private final PodOperator podOperations;

    /**
     * Constructor
     *
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public DeploymentOperator(Vertx vertx, KubernetesClient client) {
        this(vertx, client, new PodOperator(vertx, client));
    }

    /**
     * Constructor
     *
     * @param vertx             Vert.x instance
     * @param client            Kubernetes client
     * @param podOperations     Pod Operator for managing pods
     */
    public DeploymentOperator(Vertx vertx, KubernetesClient client, PodOperator podOperations) {
        super(vertx, client, "Deployment");
        this.podOperations = podOperations;
    }

    @Override
    protected MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> operation() {
        return client.apps().deployments();
    }

    @Override
    protected Integer currentScale(String namespace, String name) {
        Deployment deployment = get(namespace, name);
        if (deployment != null) {
            return deployment.getSpec().getReplicas();
        } else {
            return null;
        }
    }

    /**
     * Asynchronously roll a single-pod Deployment returning a Future which will complete once the Pod has been rolled
     * and the Deployment is ready. This is used to roll components such as Entity Operator, Kafka Exporter or Cruise
     * Control that always run with a single Pod.
     *
     * @param reconciliation        The reconciliation marker
     * @param namespace             The namespace of the deployment
     * @param name                  The name of the deployment
     * @param operationTimeoutMs    The operation timeout
     *
     * @return A future which completes when the pods in the deployment has been restarted.
     */
    public Future<Void> singlePodDeploymentRollingUpdate(Reconciliation reconciliation, String namespace, String name, long operationTimeoutMs) {
        return podOperations.listAsync(namespace, Labels.EMPTY.withStrimziName(name))
                .compose(pods -> {
                    if (pods != null && !pods.isEmpty())    {
                        return podOperations.deleteAsync(reconciliation, namespace, pods.get(0).getMetadata().getName(), true);
                    } else {
                        LOGGER.warnCr(reconciliation, "No Pods were found for Deployment {} in namespace {}", name, namespace);
                        // We return success as there is nothing to roll
                        return Future.succeededFuture();
                    }
                })
                .compose(ignored -> readiness(reconciliation, namespace, name, 1_000, operationTimeoutMs));
    }

    @Override
    protected Future<ReconcileResult<Deployment>> internalUpdate(Reconciliation reconciliation, String namespace, String name, Deployment current, Deployment desired) {
        String k8sRev = Annotations.annotations(current).get(Annotations.ANNO_DEP_KUBE_IO_REVISION);
        Annotations.annotations(desired).put(Annotations.ANNO_DEP_KUBE_IO_REVISION, k8sRev);
        return super.internalUpdate(reconciliation, namespace, name, current, desired);
    }

    /**
     * Asynchronously polls the deployment until either the observed generation matches the desired
     * generation sequence number or timeout.
     *
     * @param reconciliation The reconciliation
     * @param namespace The namespace.
     * @param name The resource name.
     * @param pollIntervalMs The polling interval
     * @param timeoutMs The timeout
     * @return  A future which completes when the observed generation of the deployment matches the
     * generation sequence number of the desired state.
     */
    public Future<Void> waitForObserved(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, "observed", pollIntervalMs, timeoutMs, this::isObserved);
    }

    /**
     * Check if a deployment has been observed.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @return Whether the deployment has been observed.
     */
    private boolean isObserved(String namespace, String name) {
        Deployment dep = get(namespace, name);
        if (dep != null
                && dep.getMetadata() != null
                && dep.getMetadata().getGeneration() != null
                && dep.getStatus() != null)   {
            return dep.getMetadata().getGeneration().equals(dep.getStatus().getObservedGeneration());
        } else {
            return false;
        }
    }
}
