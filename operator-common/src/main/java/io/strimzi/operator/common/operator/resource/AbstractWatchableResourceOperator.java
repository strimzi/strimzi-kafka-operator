/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.strimzi.operator.common.model.Labels;

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

    public final static String ANY_NAMESPACE = "*";

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

    protected Watch watchInAnyNamespace(Watcher<T> watcher) {
        return operation().inAnyNamespace().watch(watcher);
    }

    protected Watch watchInNamespace(String namespace, Watcher<T> watcher) {
        return operation().inNamespace(namespace).watch(watcher);
    }

    public Watch watch(String namespace, Watcher<T> watcher) {
        if (ANY_NAMESPACE.equals(namespace))    {
            return watchInAnyNamespace(watcher);
        } else {
            return watchInNamespace(namespace, watcher);
        }
    }

    protected Watch watchInAnyNamespace(Labels selector, Watcher<T> watcher) {
        return operation().inAnyNamespace().withLabels(selector.toMap()).watch(watcher);
    }

    protected Watch watchInNamespace(String namespace, Labels selector, Watcher<T> watcher) {
        return operation().inNamespace(namespace).withLabels(selector.toMap()).watch(watcher);
    }

    public Watch watch(String namespace, Labels selector, Watcher<T> watcher) {
        if (ANY_NAMESPACE.equals(namespace))    {
            return watchInAnyNamespace(selector, watcher);
        } else {
            return watchInNamespace(namespace, selector, watcher);
        }
    }
}
