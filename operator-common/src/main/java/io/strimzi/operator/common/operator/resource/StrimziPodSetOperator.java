/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operator for {@code StrimziPodSet}s
 */
public class StrimziPodSetOperator extends CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> {
    protected final long operationTimeoutMs;

    /**
     * Constructs the StrimziPodSet operator
     *
     * @param vertx                 The Vertx instance.
     * @param client                The Kubernetes client.
     * @param operationTimeoutMs    The timeout.
     */
    public StrimziPodSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        super(vertx, client, StrimziPodSet.class, StrimziPodSetList.class, StrimziPodSet.RESOURCE_KIND);
        this.operationTimeoutMs = operationTimeoutMs;
    }

    /**
     * Creates the StrimziPodSet and waits for the pods to become ready.
     *
     * @param reconciliation    Reconciliation marker
     * @param namespace         Namespace of the StrimziPodSet
     * @param name              Name of the StrimziPodSet
     * @param desired           The desired StrimziPodSet
     *
     * @return  Reconciliation result with the created StrimziPodSet
     */
    @Override
    protected Future<ReconcileResult<StrimziPodSet>> internalCreate(Reconciliation reconciliation, String namespace, String name, StrimziPodSet desired) {
        Future<ReconcileResult<StrimziPodSet>> create = super.internalCreate(reconciliation, namespace, name, desired);

        // If it failed, we return immediately ...
        if (create.failed()) {
            return create;
        }

        // ... if it created, we wait for the PodSet / its pods to become ready and once they do, we return the original reconciliation result.
        return create.compose(i -> waitFor(reconciliation, namespace, name, "ready", 1_000L, operationTimeoutMs, this::isReady))
                .map(create.result());
    }

    /**
     * Check if the PodSet is in the Ready state. The PodSet is ready when all the following conditions are fulfilled:
     *     - All pods are created
     *     - All pods are up-to-date
     *     - All pods are ready
     *
     * @param namespace The namespace
     * @param name      The name of the StrimziPodSet
     *
     * @return  True when the StrimziPodSet is ready. False otherwise.
     */
    public boolean isReady(String namespace, String name) {
        StrimziPodSet podSet = operation().inNamespace(namespace).withName(name).get();

        if (podSet != null) {
            int replicas = podSet.getSpec().getPods().size();

            return podSet.getStatus() != null
                    && replicas == podSet.getStatus().getPods()
                    && replicas == podSet.getStatus().getReadyPods();
        } else {
            return false;
        }
    }
}
