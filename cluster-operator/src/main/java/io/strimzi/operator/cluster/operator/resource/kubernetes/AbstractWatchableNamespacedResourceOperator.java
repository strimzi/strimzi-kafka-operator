/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.config.ConfigParameter;
import io.vertx.core.Vertx;

/**
 * Abstract class for resources which can be watched.
 *
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractWatchableNamespacedResourceOperator<
        C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>>
        extends AbstractNamespacedResourceOperator<C, T, L, R> {
    /**
     * Constructor.
     *
     * @param vertx        The vertx instance.
     * @param client       The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractWatchableNamespacedResourceOperator(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    protected Watch watchInAnyNamespace(Watcher<T> watcher) {
        return operation().inAnyNamespace().watch(watcher);
    }

    protected Watch watchInNamespace(String namespace, Watcher<T> watcher) {
        return operation().inNamespace(namespace).watch(watcher);
    }

    /**
     * Creates a resource watch
     *
     * @param namespace     Namespace which should be watched
     * @param watcher       The Watcher object which will handle the events from the watch
     *
     * @return  A Kubernetes watch instance
     */
    public Watch watch(String namespace, Watcher<T> watcher) {
        if (ConfigParameter.ANY_NAMESPACE.equals(namespace))    {
            return watchInAnyNamespace(watcher);
        } else {
            return watchInNamespace(namespace, watcher);
        }
    }

    /**
     * Creates a resource watch using a label selector
     *
     * @param namespace     Namespace which should be watched
     * @param selector      Label selector for watching only some resources
     * @param watcher       The Watcher object which will handle the events from the watch
     *
     * @return  A Kubernetes watch instance
     */
    public Watch watch(String namespace, LabelSelector selector, Watcher<T> watcher) {
        FilterWatchListDeletable<T, L, R> operation
                = ConfigParameter.ANY_NAMESPACE.equals(namespace) ? operation().inAnyNamespace() : operation().inNamespace(namespace);
        if (selector != null) {
            operation = operation.withLabelSelector(selector);
        }
        return operation.watch(watcher);
    }
}
