/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public abstract class AbstractWatchableResourceOperatorWithStatus<
        C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/,
        D extends Doneable<T>,
        R extends Resource<T, D>>
        extends AbstractWatchableResourceOperator<C, T, L, D, R> {

    public final static String ANY_NAMESPACE = "*";

    /**
     * Constructor.
     *
     * @param vertx        The vertx instance.
     * @param client       The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractWatchableResourceOperatorWithStatus(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    public abstract Future<T> updateStatusAsync(T resource);
}
