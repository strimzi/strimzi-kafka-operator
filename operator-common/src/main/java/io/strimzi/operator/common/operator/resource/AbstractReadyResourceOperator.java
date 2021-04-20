/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Specializes {@link AbstractResourceOperator} for resources which also have a notion
 * of being "ready".
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractReadyResourceOperator<C extends KubernetesClient,
            T extends HasMetadata,
            L extends KubernetesResourceList<T>,
            R extends Resource<T>>
        extends AbstractResourceOperator<C, T, L, R> {

    /**
     * Constructor.
     *
     * @param vertx        The vertx instance.
     * @param client       The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractReadyResourceOperator(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    public Future<Void> readiness(String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(namespace, name, pollIntervalMs, timeoutMs, this::isReady);
    }

    /**
     * Check if a resource is in the Ready state.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @return Whether the resource in in the Ready state.
     */
    public boolean isReady(String namespace, String name) {
        R resourceOp = operation().inNamespace(namespace).withName(name);
        T resource = resourceOp.get();
        if (resource != null)   {
            return Boolean.TRUE.equals(resourceOp.isReady());
            /*if (Readiness.isReadinessApplicable(resource.getClass())) {
                return Boolean.TRUE.equals(resourceOp.isReady());
            } else {
                log.warn("Method isReady was called on resource {} of kind {} on which readiness is not applicable.",
                        resource.getMetadata().getName(), resource.getKind());
                return true;
            }*/
        } else {
            return false;
        }
    }
}
