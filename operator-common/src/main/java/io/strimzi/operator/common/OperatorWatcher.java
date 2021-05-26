/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;

import java.util.function.Consumer;

/**
 * The fabric8 Watcher used to trigger reconciliation of an {@link Operator}.
 * @param <T> The resource type
 */
class OperatorWatcher<T extends HasMetadata> implements Watcher<T> {
    private final String namespace;
    private final Consumer<WatcherException> onClose;
    private Operator operator;
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(OperatorWatcher.class);

    OperatorWatcher(Operator operator, String namespace, Consumer<WatcherException> onClose) {
        this.namespace = namespace;
        this.onClose = onClose;
        this.operator = operator;
    }

    @Override
    public void eventReceived(Action action, T resource) {
        String name = resource.getMetadata().getName();
        String namespace = resource.getMetadata().getNamespace();
        switch (action) {
            case ADDED:
            case DELETED:
            case MODIFIED:
                Reconciliation reconciliation = new Reconciliation("watch", operator.kind(), namespace, name);
                LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", operator.kind(), name, namespace, action);
                operator.reconcile(reconciliation);
                break;
            case ERROR:
                LOGGER.errorCr(new Reconciliation("watch", operator.kind(), namespace, name), "Failed {} {} in namespace{} ", operator.kind(), name, namespace);
                operator.reconcileAll("watch error", namespace, ignored -> { });
                break;
            default:
                LOGGER.errorCr(new Reconciliation("watch", operator.kind(), namespace, name), "Unknown action: {} in namespace {}", name, namespace);
                operator.reconcileAll("watch unknown", namespace, ignored -> { });
        }
    }

    @Override
    public void onClose(WatcherException e) {
        onClose.accept(e);
    }
}
