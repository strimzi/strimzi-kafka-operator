/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <D> The doneable variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractResourceOperator<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {

    protected final Logger log = LogManager.getLogger(getClass());
    protected final Vertx vertx;
    protected final C client;
    protected final String resourceKind;

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractResourceOperator(Vertx vertx, C client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract MixedOperation<T, L, D, R> operation();

    /**
     * Asynchronously create or update the given {@code resource} depending on whether it already exists,
     * returning a future for the outcome.
     * If the resource with that name already exists the future completes successfully.
     * @param resource The resource to create.
     * @return A future which completes with the outcome.
     */
    public Future<ReconcileResult<T>> createOrUpdate(T resource) {
        if (resource == null) {
            throw new NullPointerException();
        }
        return reconcile(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), resource);
    }

    /**
     * Asynchronously reconciles the resource with the given namespace and name to match the given
     * desired resource, returning a future for the result.
     * @param namespace The namespace of the resource to reconcile
     * @param name The name of the resource to reconcile
     * @param desired The desired state of the resource.
     * @return A future which completes when the resource has been updated.
     */
    public Future<ReconcileResult<T>> reconcile(String namespace, String name, T desired) {
        if (desired != null && !namespace.equals(desired.getMetadata().getNamespace())) {
            return Future.failedFuture("Given namespace " + namespace + " incompatible with desired namespace " + desired.getMetadata().getNamespace());
        } else if (desired != null && !name.equals(desired.getMetadata().getName())) {
            return Future.failedFuture("Given name " + name + " incompatible with desired name " + desired.getMetadata().getName());
        }

        Promise<ReconcileResult<T>> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                T current = operation().inNamespace(namespace).withName(name).get();
                if (desired != null) {
                    if (current == null) {
                        log.debug("{} {}/{} does not exist, creating it", resourceKind, namespace, name);
                        internalCreate(namespace, name, desired).onComplete(future);
                    } else {
                        log.debug("{} {}/{} already exists, patching it", resourceKind, namespace, name);
                        internalPatch(namespace, name, current, desired).onComplete(future);
                    }
                } else {
                    if (current != null) {
                        // Deletion is desired
                        log.debug("{} {}/{} exist, deleting it", resourceKind, namespace, name);
                        internalDelete(namespace, name).onComplete(future);
                    } else {
                        log.debug("{} {}/{} does not exist, noop", resourceKind, namespace, name);
                        future.complete(ReconcileResult.noop(null));
                    }
                }

            },
            false,
            promise
        );
        return promise.future();
    }

    /**
     * Deletes the resource with the given namespace and name and completes the given future accordingly.
     * This method will do a cascading delete.
     *
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     *
     * @return Future with result of the reconciliation
     */
    protected Future<ReconcileResult<T>> internalDelete(String namespace, String name) {
        return internalDelete(namespace, name, true);
    }

    /**
     * Deletes the resource with the given namespace and name and completes the given future accordingly
     *
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param cascading Defines whether the delete should be cascading or not (e.g. whether a STS deletion should delete pods etc.)
     *
     * @return Future with result of the reconciliation
     */

    protected Future<ReconcileResult<T>> internalDelete(String namespace, String name, boolean cascading) {
        try {
            operation().inNamespace(namespace).withName(name).withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L).delete();
            log.debug("{} {} in namespace {} has been deleted", resourceKind, name, namespace);
            return Future.succeededFuture(ReconcileResult.deleted());
        } catch (Exception e) {
            log.debug("Caught exception while deleting {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     */
    protected Future<ReconcileResult<T>> internalPatch(String namespace, String name, T current, T desired) {
        return internalPatch(namespace, name, current, desired, true);
    }

    protected Future<ReconcileResult<T>> internalPatch(String namespace, String name, T current, T desired, boolean cascading) {
        try {
            T result = operation().inNamespace(namespace).withName(name).withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).patch(desired);
            log.debug("{} {} in namespace {} has been patched", resourceKind, name, namespace);
            return Future.succeededFuture(wasChanged(current, result) ? ReconcileResult.patched(result) : ReconcileResult.noop(result));
        } catch (Exception e) {
            log.debug("Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    protected boolean wasChanged(T oldVersion, T newVersion) {
        if (oldVersion != null
                && oldVersion.getMetadata() != null
                && newVersion != null
                && newVersion.getMetadata() != null) {
            return !Objects.equals(oldVersion.getMetadata().getResourceVersion(), newVersion.getMetadata().getResourceVersion());
        } else {
            return true;
        }
    }

    /**
     * Creates a resource with the given namespace and name with the given desired state
     * and completes the given future accordingly.
     */
    @SuppressWarnings("unchecked")
    protected Future<ReconcileResult<T>> internalCreate(String namespace, String name, T desired) {
        try {
            ReconcileResult<T> result = ReconcileResult.created(operation().inNamespace(namespace).withName(name).create(desired));
            log.debug("{} {} in namespace {} has been created", resourceKind, name, namespace);
            return Future.succeededFuture(result);
        } catch (Exception e) {
            log.debug("Caught exception while creating {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Synchronously gets the resource with the given {@code name} in the given {@code namespace}.
     * @param namespace The namespace.
     * @param name The name.
     * @return The resource, or null if it doesn't exist.
     */
    public T get(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name).get();
    }

    /**
     * Asynchronously gets the resource with the given {@code name} in the given {@code namespace}.
     * @param namespace The namespace.
     * @param name The name.
     * @return A Future for the result.
     */
    public Future<T> getAsync(String namespace, String name) {
        Promise<T> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                T resource = get(namespace, name);
                future.complete(resource);
            }, true, result
        );
        return result.future();
    }

    /**
     * Synchronously list the resources in the given {@code namespace} with the given {@code selector}.
     * @param namespace The namespace.
     * @param selector The selector.
     * @return A list of matching resources.
     */
    @SuppressWarnings("unchecked")
    public List<T> list(String namespace, Labels selector) {
        if (AbstractWatchableResourceOperator.ANY_NAMESPACE.equals(namespace))  {
            return listInAnyNamespace(selector);
        } else {
            return listInNamespace(namespace, selector);
        }
    }

    @SuppressWarnings("unchecked") // due to L extends KubernetesResourceList/*<T>*/
    protected List<T> listInAnyNamespace(Labels selector) {
        FilterWatchListMultiDeletable<T, L, Boolean, Watch, Watcher<T>> operation = operation().inAnyNamespace();

        if (selector != null) {
            Map<String, String> labels = selector.toMap();
            return operation.withLabels(labels)
                    .list()
                    .getItems();
        } else {
            return operation
                    .list()
                    .getItems();
        }
    }

    @SuppressWarnings("unchecked") // due to L extends KubernetesResourceList/*<T>*/
    protected List<T> listInNamespace(String namespace, Labels selector) {
        NonNamespaceOperation<T, L, D, R> tldrNonNamespaceOperation = operation().inNamespace(namespace);

        if (selector != null) {
            Map<String, String> labels = selector.toMap();
            FilterWatchListDeletable<T, L, Boolean, Watch, Watcher<T>> tlBooleanWatchWatcherFilterWatchListDeletable = tldrNonNamespaceOperation.withLabels(labels);
            return tlBooleanWatchWatcherFilterWatchListDeletable
                    .list()
                    .getItems();
        } else {
            return tldrNonNamespaceOperation
                    .list()
                    .getItems();
        }
    }

    /**
     * Asynchronously lists the resource with the given {@code selector} in the given {@code namespace}.
     *
     * @param namespace The namespace.
     * @param selector The selector.
     * @return A Future with a list of matching resources.
     */
    public Future<List<T>> listAsync(String namespace, Labels selector) {
        Promise<List<T>> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                List<T> resources;

                if (AbstractWatchableResourceOperator.ANY_NAMESPACE.equals(namespace))  {
                    resources = listInAnyNamespace(selector);
                } else {
                    resources = listInNamespace(namespace, selector);
                }

                future.complete(resources);
            }, true, result
        );
        return result.future();
    }

    @SuppressWarnings("unchecked")
    public Future<List<T>> listAsync(String namespace, Optional<LabelSelector> selector) {
        Promise<List<T>> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                FilterWatchListDeletable<T, L, Boolean, Watch, Watcher<T>> operation;
                if (AbstractWatchableResourceOperator.ANY_NAMESPACE.equals(namespace))  {
                    operation = operation().inAnyNamespace();
                } else {
                    operation = operation().inNamespace(namespace);
                }
                if (selector.isPresent()) {
                    operation = operation.withLabelSelector(selector.get());
                }
                future.complete(operation.list().getItems());
            }, true, result
        );
        return result.future();
    }

    /**
     * Returns a future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param predicate The predicate.
     * @return A future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     */
    public Future<Void> waitFor(String namespace, String name, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
        return waitFor(namespace, name, "ready", pollIntervalMs, timeoutMs, predicate);
    }

    /**
     * Returns a future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     *
     * @param namespace The namespace.
     * @param name The resource name.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param predicate The predicate.
     * @return A future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     */
    public Future<Void> waitFor(String namespace, String name, String logState, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
        return Util.waitFor(vertx,
            String.format("%s resource %s in namespace %s", resourceKind, name, namespace),
            logState,
            pollIntervalMs,
            timeoutMs,
            () -> predicate.test(namespace, name));
    }
}
