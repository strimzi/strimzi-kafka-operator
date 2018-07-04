/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

public abstract class AbstractWatchableResourceOperator<
        C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/,
        D extends Doneable<T>,
        R extends Resource<T, D>>
        extends AbstractResourceOperator<C, T, L, D, R> {

    /**
     * Constructor.
     *
     * @param vertx        The vertx instance.
     * @param client       The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractWatchableResourceOperator(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    public Watch watch(String namespace, Watcher<T> watcher) {
        return operation().inNamespace(namespace).watch(watcher);
    }
}
