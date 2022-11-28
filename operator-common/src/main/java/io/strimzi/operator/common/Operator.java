/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.operator.common.metrics.OperatorMetricsHolder;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Abstraction of an operator which is driven by resources of a given {@link #kind()}.
 *
 * {@link #reconcile(Reconciliation)} triggers the asynchronous reconciliation of a named resource.
 * Reconciliation of a given resource may be triggered either by a Kubernetes watch event (see {@link OperatorWatcher}) or
 * on a regular schedule.
 * {@link #reconcileAll(String, String, Handler)} triggers reconciliation of all the resources that the operator consumes.
 * An operator instance is not bound to a particular namespace. Rather the namespace is passed as a parameter.
 */
public interface Operator {
    /**
     * The Kubernetes kind of the resource "consumed" by this operator
     * @return The kind.
     */
    String kind();

    /**
     * Returns the operator metrics holder which is used to hold the operator metrics
     *
     * @return  Metrics holder instance
     */
    OperatorMetricsHolder metrics();

    /**
     * Reconcile the resource identified by the given reconciliation.
     * @param reconciliation The resource.
     * @return A Future is completed once the resource has been reconciled.
     */
    Future<Void> reconcile(Reconciliation reconciliation);

    /**
     * Triggers the asynchronous reconciliation of all resources which this operator consumes.
     * The resources to reconcile are identified by {@link #allResourceNames(String)}.
     * @param trigger The cause of this reconciliation (for logging).
     * @param namespace The namespace to reconcile, or {@code *} to reconcile across all namespaces.
     * @param handler Handler called on completion.
     */
    default void reconcileAll(String trigger, String namespace, Handler<AsyncResult<Void>> handler) {
        allResourceNames(namespace).onComplete(ar -> {
            if (ar.succeeded()) {
                reconcileThese(trigger, ar.result(), namespace, handler);
                metrics().periodicReconciliationsCounter(namespace).increment();
            } else {
                handler.handle(ar.map((Void) null));
            }
        });
    }

    /**
     * Reconciles a set of resources
     *
     * @param trigger       The cause of this reconciliation (for logging).
     * @param desiredNames  Set of resources which should be reconciled
     * @param namespace     The namespace to reconcile, or {@code *} to reconcile across all namespaces.
     * @param handler       Handler called on completion.
     */
    default void reconcileThese(String trigger, Set<NamespaceAndName> desiredNames, String namespace, Handler<AsyncResult<Void>> handler) {
        if (namespace.equals("*")) {
            metrics().resetResourceAndPausedResourceCounters();
        } else {
            metrics().resourceCounter(namespace).set(0);
            metrics().pausedResourceCounter(namespace).set(0);
        }

        if (desiredNames.size() > 0) {
            @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
            List<Future> futures = new ArrayList<>();
            for (NamespaceAndName resourceRef : desiredNames) {
                metrics().resourceCounter(resourceRef.getNamespace()).getAndIncrement();
                Reconciliation reconciliation = new Reconciliation(trigger, kind(), resourceRef.getNamespace(), resourceRef.getName());
                futures.add(reconcile(reconciliation));
            }
            CompositeFuture.join(futures).map((Void) null).onComplete(handler);
        } else {
            handler.handle(Future.succeededFuture());
        }
    }

    /**
     * Returns a future which completes with the names of all the resources to be reconciled by
     * {@link #reconcileAll(String, String, Handler)}.
     *
     * @param namespace The namespace
     * @return The set of resource names
     */
    Future<Set<NamespaceAndName>> allResourceNames(String namespace);

    /**
     * A selector for narrowing the resources which this operator instance consumes to those whose labels match this selector.
     * @return A selector.
     */
    default Optional<LabelSelector> selector() {
        return Optional.empty();
    }
}
