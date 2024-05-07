/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractWatchableNamespacedResourceOperator;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.function.BiConsumer;

/**
 * The Fabric8 Watcher which automatically reconnects when it is closed with an error.
 *
 * @param <T> The resource type
 */
public class ReconnectingWatcher<T extends HasMetadata> implements Watcher<T> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ReconnectingWatcher.class);

    private final AbstractWatchableNamespacedResourceOperator<?, T, ?, ?> resourceOperator;
    private final String kind;
    private final String namespace;
    private final LabelSelector selector;
    private final BiConsumer<Action, T> eventHandler;

    private Watch watch;

    /**
     * Creates an automatically reconnecting watch
     *
     * @param resourceOperator  Operator for managing the resource which should be watched
     * @param kind              Kind of the resource this watcher is watching
     * @param namespace         Namespace which should be watched (or * for all namespaces)
     * @param selector          Custom resource selector
     * @param eventHandler      Event handler for handling the received events
     */
    public ReconnectingWatcher(AbstractWatchableNamespacedResourceOperator<?, T, ?, ?> resourceOperator, String kind, String namespace, LabelSelector selector, BiConsumer<Action, T> eventHandler) {
        this.resourceOperator = resourceOperator;
        this.kind = kind;
        this.namespace = namespace;
        this.selector = selector;
        this.eventHandler = eventHandler;

        this.watch = createWatch();
    }

    @Override
    public void eventReceived(Action action, T resource) {
        eventHandler.accept(action, resource);
    }

    @Override
    public void onClose(WatcherException e) {
        LOGGER.warnOp("Watch for resource {} in namespace {} with selector {} failed and will be reconnected", kind, namespace, selector,  e);
        watch = createWatch(); // We recreate the watch
    }

    /**
     * (Re)creates the watch
     *
     * @return  The created watch
     */
    private Watch createWatch() {
        return resourceOperator.watch(namespace, selector, this);
    }

    /**
     * Closes the watch
     */
    public void close() {
        watch.close();
    }
}
