/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

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
public abstract class AbstractNonNamespacedResourceOperator<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {

    protected final Logger log = LogManager.getLogger(getClass());
    protected final Vertx vertx;
    protected final C client;
    protected final String resourceKind;
    private final long operationTimeoutMs;

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     * @param operationTimeoutMs Timeout for operations.
     */
    public AbstractNonNamespacedResourceOperator(Vertx vertx, C client, String resourceKind, long operationTimeoutMs) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    protected abstract NonNamespaceOperation<T, L, D, R> operation();

    /**
     * Asynchronously create or update the given {@code resource} depending on whether it already exists,
     * returning a future for the outcome.
     * If the resource with that name already exists the future completes successfully.
     * @param resource The resource to create.
     * @return A future which completes when the resource was created or updated.
     */
    public Future<ReconcileResult<T>> createOrUpdate(T resource) {
        if (resource == null) {
            throw new NullPointerException();
        }
        return reconcile(resource.getMetadata().getName(), resource);
    }

    /**
     * Asynchronously reconciles the resource with the given name to match the given
     * desired resource, returning a future for the result.
     * @param name The name of the resource to reconcile.
     * @param desired The desired state of the resource.
     * @return A future which completes when the resource was reconciled.
     */
    public Future<ReconcileResult<T>> reconcile(String name, T desired) {

        if (desired != null && !name.equals(desired.getMetadata().getName())) {
            return Future.failedFuture("Given name " + name + " incompatible with desired name "
                    + desired.getMetadata().getName());
        }

        Promise<ReconcileResult<T>> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                T current = operation().withName(name).get();
                if (desired != null) {
                    if (current == null) {
                        log.debug("{} {} does not exist, creating it", resourceKind, name);
                        internalCreate(name, desired).onComplete(future);
                    } else {
                        log.debug("{} {} already exists, patching it", resourceKind, name);
                        internalPatch(name, current, desired).onComplete(future);
                    }
                } else {
                    if (current != null) {
                        // Deletion is desired
                        log.debug("{} {} exist, deleting it", resourceKind, name);
                        internalDelete(name).onComplete(future);
                    } else {
                        log.debug("{} {} does not exist, noop", resourceKind, name);
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
     * Asynchronously deletes the resource with the given {@code name}.
     * @param name The resource to be deleted.
     * @return A future which will be completed on the context thread
     * once the resource has been deleted.
     */
    private Future<ReconcileResult<T>> internalDelete(String name) {
        Future<ReconcileResult<T>> watchForDeleteFuture = new ResourceSupport(vertx).selfClosingWatch(operation().withName(name),
            (action, resource) -> {
                if (action == Watcher.Action.DELETED) {
                    log.debug("{} {} has been deleted", resourceKind, name);
                    return ReconcileResult.deleted();
                } else {
                    return null;
                }
            }, operationTimeoutMs);

        Future<Void> deleteFuture = deleteAsync(name);

        return CompositeFuture.join(watchForDeleteFuture, deleteFuture).map(ReconcileResult.deleted());
    }

    private Future<Void> deleteAsync(String name) {
        Promise<Void> deletePromise = Promise.promise();
        vertx.executeBlocking(
            f -> {
                try {
                    Boolean delete = operation().withName(name).withGracePeriod(-1L).delete();
                    if (!Boolean.TRUE.equals(delete)) {
                        f.fail(new RuntimeException(resourceKind + "/" + name + " could not be deleted (returned " + delete + ")"));
                    } else {
                        f.complete();
                    }
                } catch (Throwable t) {
                    f.fail(t);
                }
            },
            true,
            deletePromise);
        return deletePromise.future();
    }

    /**
     * Patches the resource with the given name to match the given desired resource
     * and completes the given future accordingly.
     */
    protected Future<ReconcileResult<T>> internalPatch(String name, T current, T desired) {
        return internalPatch(name, current, desired, true);
    }

    protected Future<ReconcileResult<T>> internalPatch(String name, T current, T desired, boolean cascading) {
        try {
            T result = operation().withName(name).withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).patch(desired);
            log.debug("{} {} has been patched", resourceKind, name);
            return Future.succeededFuture(wasChanged(current, result) ?
                    ReconcileResult.patched(result) : ReconcileResult.noop(result));
        } catch (Exception e) {
            log.debug("Caught exception while patching {} {}", resourceKind, name, e);
            return Future.failedFuture(e);
        }
    }

    private boolean wasChanged(T oldVersion, T newVersion) {
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
     * Creates a resource with the name with the given desired state
     * and completes the given future accordingly.
     */
    @SuppressWarnings("unchecked")
    protected Future<ReconcileResult<T>> internalCreate(String name, T desired) {
        try {
            ReconcileResult<T> result = ReconcileResult.created(operation().withName(name).create(desired));
            log.debug("{} {} has been created", resourceKind, name);
            return Future.succeededFuture(result);
        } catch (Exception e) {
            log.debug("Caught exception while creating {} {}", resourceKind, name, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Synchronously gets the resource with the given {@code name}.
     * @param name The name.
     * @return The resource, or null if it doesn't exist.
     */
    public T get(String name) {
        return operation().withName(name).get();
    }

    /**
     * Asynchronously gets the resource with the given {@code name}.
     * @param name The name.
     * @return A Future for the result.
     */
    public Future<T> getAsync(String name) {
        Promise<T> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                T resource = get(name);
                future.complete(resource);
            }, true, result
        );
        return result.future();
    }

    /**
     * Synchronously list the resources with the given {@code selector}.
     * @param selector The selector.
     * @return A list of matching resources.
     */
    public List<T> list(Labels selector) {
        return listInAnyNamespace(selector);
    }

    /**
     * Asynchronously list the resources with the given {@code selector}.
     * @param selector The selector.
     * @return A list of matching resources.
     */
    public Future<List<T>> listAsync(Labels selector) {
        Promise<List<T>> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                List<T> resource = list(selector);
                future.complete(resource);
            }, true, result
        );
        return result.future();
    }

    @SuppressWarnings("unchecked") // due to L extends KubernetesResourceList/*<T>*/
    protected List<T> listInAnyNamespace(Labels selector) {
        FilterWatchListMultiDeletable<T, L, Boolean, Watch, Watcher<T>> operation = operation();

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

    /**
     * Returns a future that completes when the resource identified by the given {@code name}
     * is ready.
     *
     * @param name The resource name.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param predicate The predicate.
     * @return a future that completes when the resource identified by the given {@code name} is ready.
     */
    public Future<Void> waitFor(String name, String logState, long pollIntervalMs, final long timeoutMs, Predicate<String> predicate) {
        return Util.waitFor(vertx,
            String.format("%s resource %s", resourceKind, name),
            logState,
            pollIntervalMs,
            timeoutMs,
            () -> predicate.test(name));
    }
}
