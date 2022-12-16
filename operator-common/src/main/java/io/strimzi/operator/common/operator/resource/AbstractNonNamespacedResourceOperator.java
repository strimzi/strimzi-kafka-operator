/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.List;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractNonNamespacedResourceOperator<C extends KubernetesClient,
            T extends HasMetadata,
            L extends KubernetesResourceList<T>,
            R extends Resource<T>>
        extends AbstractResourceOperator<C, T, L, R> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractNonNamespacedResourceOperator.class);

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractNonNamespacedResourceOperator(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    protected abstract NonNamespaceOperation<T, L, R> operation();

    /**
     * Asynchronously create or update the given {@code resource} depending on whether it already exists,
     * returning a future for the outcome.
     * If the resource with that name already exists the future completes successfully.
     * @param reconciliation The reconciliation
     * @param resource The resource to create.
     * @return A future which completes when the resource was created or updated.
     */
    public Future<ReconcileResult<T>> createOrUpdate(Reconciliation reconciliation, T resource) {
        if (resource == null) {
            throw new NullPointerException();
        }
        return reconcile(reconciliation, resource.getMetadata().getName(), resource);
    }

    /**
     * Asynchronously reconciles the resource with the given name to match the given
     * desired resource, returning a future for the result.
     * @param reconciliation The reconciliation
     * @param name The name of the resource to reconcile.
     * @param desired The desired state of the resource.
     * @return A future which completes when the resource was reconciled.
     */
    public Future<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String name, T desired) {
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
                        LOGGER.debugCr(reconciliation, "{} {} does not exist, creating it", resourceKind, name);
                        internalCreate(reconciliation, name, desired).onComplete(future);
                    } else {
                        LOGGER.debugCr(reconciliation, "{} {} already exists, patching it", resourceKind, name);
                        internalPatch(reconciliation, name, current, desired).onComplete(future);
                    }
                } else {
                    if (current != null) {
                        // Deletion is desired
                        LOGGER.debugCr(reconciliation, "{} {} exist, deleting it", resourceKind, name);
                        internalDelete(reconciliation, name).onComplete(future);
                    } else {
                        LOGGER.debugCr(reconciliation, "{} {} does not exist, noop", resourceKind, name);
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
     * Asynchronously deletes the resource with the given {@code name},
     * returning a Future which completes once the resource
     * is observed to have been deleted.
     * @param reconciliation The reconciliation
     * @param name The resource to be deleted.
     * @return A future which will be completed on the context thread
     * once the resource has been deleted.
     */
    private Future<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String name) {
        R resourceOp = operation().withName(name);

        Future<ReconcileResult<T>> watchForDeleteFuture = resourceSupport.selfClosingWatch(
            reconciliation,
            resourceOp,
            resourceOp,
            deleteTimeoutMs(),
            "observe deletion of " + resourceKind + " " + name,
            (action, resource) -> {
                if (action == Watcher.Action.DELETED) {
                    LOGGER.debugCr(reconciliation, "{} {} has been deleted", resourceKind, name);
                    return ReconcileResult.deleted();
                } else {
                    return null;
                }
            },
            resource -> {
                if (resource == null) {
                    LOGGER.debugCr(reconciliation, "{} {} has been already deleted in pre-check", resourceKind, name);
                    return ReconcileResult.deleted();
                } else {
                    return null;
                }
            });

        Future<Void> deleteFuture = resourceSupport.deleteAsync(resourceOp);

        return CompositeFuture.join(watchForDeleteFuture, deleteFuture).map(ReconcileResult.deleted());
    }

    /**
     * Patches the resource with the given name to match the given desired resource
     * and completes the given future accordingly.
     */
    protected Future<ReconcileResult<T>> internalPatch(Reconciliation reconciliation, String name, T current, T desired) {
        if (needsPatching(reconciliation, name, current, desired))  {
            try {
                T result = operation().withName(name).patch(PatchContext.of(PatchType.JSON), desired);
                LOGGER.debugCr(reconciliation, "{} {} has been patched", resourceKind, name);

                return Future.succeededFuture(wasChanged(current, result) ?
                        ReconcileResult.patched(result) : ReconcileResult.noop(result));
            } catch (Exception e) {
                LOGGER.debugCr(reconciliation, "Caught exception while patching {} {}", resourceKind, name, e);
                return Future.failedFuture(e);
            }
        } else {
            LOGGER.debugCr(reconciliation, "{} {} did not changed and doesn't need patching", resourceKind, name);
            return Future.succeededFuture(ReconcileResult.noop(current));
        }
    }

    /**
     * Creates a resource with the name with the given desired state
     * and completes the given future accordingly.
     */
    protected Future<ReconcileResult<T>> internalCreate(Reconciliation reconciliation, String name, T desired) {
        try {
            ReconcileResult<T> result = ReconcileResult.created(operation().resource(desired).create());
            LOGGER.debugCr(reconciliation, "{} {} has been created", resourceKind, name);

            return Future.succeededFuture(result);
        } catch (Exception e) {
            LOGGER.debugCr(reconciliation, "Caught exception while creating {} {}", resourceKind, name, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Synchronously gets the resource with the given {@code name}.
     * @param name The name.
     * @return The resource, or null if it doesn't exist.
     */
    public T get(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(resourceKind + " with an empty name cannot be configured. Please provide a name.");
        }
        return operation().withName(name).get();
    }

    /**
     * Asynchronously gets the resource with the given {@code name}.
     * @param name The name.
     * @return A Future for the result.
     */
    public Future<T> getAsync(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(resourceKind + " with an empty name cannot be configured. Please provide a name.");
        }
        return resourceSupport.getAsync(operation().withName(name));
    }

    /**
     * Synchronously list the resources with the given {@code selector}.
     * @param selector The selector.
     * @return A list of matching resources.
     */
    public List<T> list(Labels selector) {
        return list(applySelector(operation(), selector));
    }

    /**
     * Asynchronously list the resources with the given {@code selector}.
     * @param selector The selector.
     * @return A list of matching resources.
     */
    public Future<List<T>> listAsync(Labels selector) {
        return listAsync(applySelector(operation(), selector));
    }
}
