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
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractNamespacedResourceOperator<C extends KubernetesClient,
            T extends HasMetadata,
            L extends KubernetesResourceList<T>,
            R extends Resource<T>>
        extends AbstractResourceOperator<C, T, L, R> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractNamespacedResourceOperator.class);

    /**
     * Marker for indication "all namesapces" => this is used for example when creating watches to create a cluster
     * wide watch.
     */
    public final static String ANY_NAMESPACE = "*";

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractNamespacedResourceOperator(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    protected abstract MixedOperation<T, L, R> operation();

    /**
     * Asynchronously create or update the given {@code resource} depending on whether it already exists,
     * returning a future for the outcome.
     * If the resource with that name already exists the future completes successfully.
     * @param reconciliation The reconciliation
     * @param resource The resource to create.
     * @return A future which completes with the outcome.
     */
    public Future<ReconcileResult<T>> createOrUpdate(Reconciliation reconciliation, T resource) {
        if (resource == null) {
            throw new NullPointerException();
        }
        return reconcile(reconciliation, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), resource);
    }

    /**
     * Asynchronously reconciles the resource with the given namespace and name to match the given
     * desired resource, returning a future for the result.
     * @param reconciliation Reconciliation object
     * @param namespace The namespace of the resource to reconcile
     * @param name The name of the resource to reconcile
     * @param desired The desired state of the resource.
     * @return A future which completes when the resource has been updated.
     */
    public Future<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String namespace, String name, T desired) {
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
                        LOGGER.debugCr(reconciliation, "{} {}/{} does not exist, creating it", resourceKind, namespace, name);
                        internalCreate(reconciliation, namespace, name, desired).onComplete(future);
                    } else {
                        LOGGER.debugCr(reconciliation, "{} {}/{} already exists, patching it", resourceKind, namespace, name);
                        internalPatch(reconciliation, namespace, name, current, desired).onComplete(future);
                    }
                } else {
                    if (current != null) {
                        // Deletion is desired
                        LOGGER.debugCr(reconciliation, "{} {}/{} exist, deleting it", resourceKind, namespace, name);
                        internalDelete(reconciliation, namespace, name).onComplete(future);
                    } else {
                        LOGGER.debugCr(reconciliation, "{} {}/{} does not exist, noop", resourceKind, namespace, name);
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
     * Does a batch reconciliation of resources. It takes a list with desired resources and a selector for getting all
     * resources. It will compare the desired resources against the actual resources based on the selector and decides
     * which need to be created, modified or deleted. This is useful in situations when we need to manage list of
     * resources per operand and not just single resource which either exists or not. The reconciliation of the
     * individual resources delegates to the regular reconcile(...) methods for a single resource.
     *
     * @param reconciliation    Reconciliation marker
     * @param namespace         Namespace where the resources should be reconciled
     * @param desired           List of desired resources
     * @param selector          Selector for getting a list of current resource
     *
     * @return  Future which completes when the lists are reconciled
     */
    public Future<Void> batchReconcile(Reconciliation reconciliation, String namespace, List<T> desired, Labels selector)  {
        return listAsync(namespace, selector)
                .compose(current -> {
                    @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                    List<Future> futures = new ArrayList<>(desired.size());
                    List<String> currentNames = current.stream().map(ingress -> ingress.getMetadata().getName()).collect(Collectors.toList());

                    LOGGER.debugCr(reconciliation, "Reconciling existing {} resources {} against the desired {} resources", resourceKind, currentNames, resourceKind);

                    // Update desired resources which should be created or already exist and are still desired
                    for (T desiredResource : desired) {
                        String name = desiredResource.getMetadata().getName();
                        currentNames.remove(name);
                        futures.add(reconcile(reconciliation, namespace, name, desiredResource));
                    }

                    LOGGER.debugCr(reconciliation, "{} {}/{} should be deleted", resourceKind, namespace, currentNames);

                    // Delete resources which match our selector but are not desired anymore
                    for (String name : currentNames) {
                        futures.add(reconcile(reconciliation, namespace, name, null));
                    }

                    return CompositeFuture
                            .join(futures)
                            .map((Void) null);
                });
    }

    /**
     * Deletes the resource with the given namespace and name and completes the given future accordingly.
     * This method will do a cascading delete.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     *
     * @return A future which will be completed on the context thread
     *         once the resource has been deleted.
     */
    protected Future<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String namespace, String name) {
        return internalDelete(reconciliation, namespace, name, true);
    }

    /**
     * Asynchronously deletes the resource in the given {@code namespace} with the given {@code name},
     * returning a Future which completes once the resource
     * is observed to have been deleted.
     *
     * @param reconciliation The reconciliation
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param cascading Defines whether the delete should be cascading or not (e.g. whether a STS deletion should delete pods etc.)
     *
     * @return A future which will be completed on the context thread
     *         once the resource has been deleted.
     */
    protected Future<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        R resourceOp = operation().inNamespace(namespace).withName(name);

        Future<ReconcileResult<T>> watchForDeleteFuture = resourceSupport.selfClosingWatch(
            reconciliation,
            resourceOp,
            resourceOp,
            deleteTimeoutMs(),
            "observe deletion of " + resourceKind + " " + namespace + "/" + name,
            (action, resource) -> {
                if (action == Watcher.Action.DELETED) {
                    LOGGER.debugCr(reconciliation, "{} {}/{} has been deleted", resourceKind, namespace, name);
                    return ReconcileResult.deleted();
                } else {
                    return null;
                }
            },
            resource -> {
                if (resource == null) {
                    LOGGER.debugCr(reconciliation, "{} {}/{} has been already deleted in pre-check", resourceKind, namespace, name);
                    return ReconcileResult.deleted();
                } else {
                    return null;
                }
            });

        Future<Void> deleteFuture = resourceSupport.deleteAsync(resourceOp.withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L));

        return CompositeFuture.join(watchForDeleteFuture, deleteFuture).map(ReconcileResult.deleted());
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     */
    protected Future<ReconcileResult<T>> internalPatch(Reconciliation reconciliation, String namespace, String name, T current, T desired) {
        if (needsPatching(reconciliation, name, current, desired))  {
            try {
                T result = operation().inNamespace(namespace).withName(name).patch(PatchContext.of(PatchType.JSON), desired);
                LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been patched", resourceKind, name, namespace);
                return Future.succeededFuture(wasChanged(current, result) ? ReconcileResult.patched(result) : ReconcileResult.noop(result));
            } catch (Exception e) {
                LOGGER.debugCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
                return Future.failedFuture(e);
            }
        } else {
            LOGGER.debugCr(reconciliation, "{} {} in namespace {} did not changed and doesn't need patching", resourceKind, name, namespace);
            return Future.succeededFuture(ReconcileResult.noop(current));
        }
    }

    /**
     * Creates a resource with the given namespace and name with the given desired state
     * and completes the given future accordingly.
     */
    protected Future<ReconcileResult<T>> internalCreate(Reconciliation reconciliation, String namespace, String name, T desired) {
        try {
            ReconcileResult<T> result = ReconcileResult.created(operation().inNamespace(namespace).resource(desired).create());
            LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been created", resourceKind, name, namespace);
            return Future.succeededFuture(result);
        } catch (Exception e) {
            LOGGER.debugCr(reconciliation, "Caught exception while creating {} {} in namespace {}", resourceKind, name, namespace, e);
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
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(namespace + "/" + resourceKind + " with an empty name cannot be configured. Please provide a name.");
        }
        return operation().inNamespace(namespace).withName(name).get();
    }

    /**
     * Asynchronously gets the resource with the given {@code name} in the given {@code namespace}.
     * @param namespace The namespace.
     * @param name The name.
     * @return A Future for the result.
     */
    public Future<T> getAsync(String namespace, String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(namespace + "/" + resourceKind + " with an empty name cannot be configured. Please provide a name.");
        }
        return resourceSupport.getAsync(operation().inNamespace(namespace).withName(name));
    }

    /**
     * Synchronously list the resources in the given {@code namespace} with the given {@code selector}.
     * @param namespace The namespace.
     * @param selector The selector.
     * @return A list of matching resources.
     */
    public List<T> list(String namespace, Labels selector) {
        return list(applySelector(applyNamespace(namespace), selector));
    }

    /**
     * Applies the namespace on the operation. Depending on the value of the namespace parameter, it returns either
     * operation for working in all namespaces or in the selected namespace.
     *
     * @param namespace     Namespace which should be applied or * for all namespaces
     *
     * @return  Operation with applied namespace
     */
    private FilterWatchListDeletable<T, L, R> applyNamespace(String namespace)  {
        if (ANY_NAMESPACE.equals(namespace))  {
            return operation().inAnyNamespace();
        } else {
            return operation().inNamespace(namespace);
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
        return listAsync(applySelector(applyNamespace(namespace), selector));
    }

    /**
     * Asynchronously lists the resource with the given {@code selector} in the given {@code namespace}.
     *
     * @param namespace     Namespace where the resources should be listed
     * @param selector      Label selector for selecting only some of the resources
     *
     * @return A Future with a list of matching resources.
     */
    public Future<List<T>> listAsync(String namespace, Optional<LabelSelector> selector) {
        return listAsync(applySelector(applyNamespace(namespace), selector));
    }

    /**
     * Returns a future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     *
     * @param reconciliation The reconciliation
     * @param namespace The namespace.
     * @param name The resource name.
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param predicate The predicate.
     * @return A future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     */
    public Future<Void> waitFor(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
        return waitFor(reconciliation, namespace, name, "ready", pollIntervalMs, timeoutMs, predicate);
    }

    /**
     * Returns a future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     *
     * @param reconciliation The reconciliation
     * @param namespace The namespace.
     * @param name The resource name.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param predicate The predicate.
     * @return A future that completes when the resource identified by the given {@code namespace} and {@code name}
     * is ready.
     */
    public Future<Void> waitFor(Reconciliation reconciliation, String namespace, String name, String logState, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
        return Util.waitFor(reconciliation, vertx,
            String.format("%s resource %s in namespace %s", resourceKind, name, namespace),
            logState,
            pollIntervalMs,
            timeoutMs,
            () -> predicate.test(namespace, name));
    }

    /**
     * Asynchronously deletes the resource with the given {@code name} in the given {@code namespace}.
     *
     * @param reconciliation    The reconciliation
     * @param namespace         Namespace of the resource which should be deleted
     * @param name              Name of the resource which should be deleted
     * @param cascading         Defines whether the deletion should be cascading or not
     *
     * @return                  A Future with True if the deletion succeeded and False when it failed.
     */
    public Future<Void> deleteAsync(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        return internalDelete(reconciliation, namespace, name, cascading).map((Void) null);
    }

    /**
     * Creates the informer for given resource type to inform on all instances in given namespace (or cluster-wide).
     *
     * @param namespace Namespace on which to inform
     *
     * @return          Informer instance
     */
    public SharedIndexInformer<T> informer(String namespace)   {
        if (ANY_NAMESPACE.equals(namespace))    {
            return operation().inAnyNamespace().inform();
        } else {
            return operation().inNamespace(namespace).inform();
        }
    }

    /**
     * Creates the informer for given resource type to inform on all instances in given namespace (or cluster-wide)
     * matching the selector.
     *
     * @param namespace         Namespace on which to inform
     * @param selectorLabels    Selector which should be matched by the resources
     *
     * @return                  Informer instance
     */
    public SharedIndexInformer<T> informer(String namespace, Map<String, String> selectorLabels)   {
        if (ANY_NAMESPACE.equals(namespace))    {
            return operation().inAnyNamespace().withLabels(selectorLabels).inform();
        } else {
            return operation().inNamespace(namespace).withLabels(selectorLabels).inform();
        }
    }

    /**
     * Returns the Kubernetes client for given resource type
     *
     * @return  Kubernetes client instance for given resource
     */
    public MixedOperation<T, L, R> client() {
        return operation();
    }
}
