/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.Informable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
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
     * Constructor.
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client        The kubernetes client.
     * @param resourceKind  The mind of Kubernetes resource (used for logging).
     */
    protected AbstractNamespacedResourceOperator(Executor asyncExecutor, C client, String resourceKind) {
        super(asyncExecutor, client, resourceKind);
    }

    protected abstract MixedOperation<T, L, R> operation();

    /**
     * Asynchronously create or update the given {@code resource} depending on whether it already exists,
     * returning a CompletionStage for the outcome.
     * If the resource with that name already exists the future completes successfully.
     * @param reconciliation The reconciliation
     * @param resource The resource to create.
     * @return A CompletionStage which completes with the outcome.
     */
    public CompletionStage<ReconcileResult<T>> createOrUpdate(Reconciliation reconciliation, T resource) {
        if (resource == null) {
            throw new NullPointerException();
        }
        return reconcile(reconciliation, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), resource);
    }

    /**
     * Asynchronously reconciles the resource with the given namespace and name to
     * match the given desired resource, returning a CompletionStage for the result.
     *
     * @param reconciliation Reconciliation object
     * @param namespace      The namespace of the resource to reconcile
     * @param name           The name of the resource to reconcile
     * @param desired        The desired state of the resource.
     * @return A CompletionStage which completes when the resource has been updated.
     */
    public CompletionStage<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String namespace, String name, T desired) {
        if (desired != null && !namespace.equals(desired.getMetadata().getNamespace())) {
            return CompletableFuture.failedStage(new IllegalArgumentException("Given namespace " + namespace + " incompatible with desired namespace " + desired.getMetadata().getNamespace()));
        } else if (desired != null && !name.equals(desired.getMetadata().getName())) {
            return CompletableFuture.failedStage(new IllegalArgumentException("Given name " + name + " incompatible with desired name " + desired.getMetadata().getName()));
        }

        return CompletableFuture.supplyAsync(() -> operation().inNamespace(namespace).withName(name).get(), asyncExecutor)
            .thenCompose(current -> this.reconcile(reconciliation, namespace, name, current, desired));
    }

    /**
     * Asynchronously reconciles the given current resource to match the given
     * desired resource, returning a CompletionStage for the result.
     *
     * @param reconciliation Reconciliation object
     * @param namespace      The namespace of the resource to reconcile
     * @param name           The name of the resource to reconcile
     * @param current        The current state of the resource.
     * @param desired        The desired state of the resource.
     * @return A CompletionStage which completes when the resource has been updated.
     */
    public CompletionStage<ReconcileResult<T>> reconcile(Reconciliation reconciliation, String namespace, String name, T current, T desired) {
        if (desired != null) {
            if (current == null) {
                LOGGER.debugCr(reconciliation, "{} {}/{} does not exist, creating it", resourceKind, namespace, name);
                return internalCreate(reconciliation, namespace, name, desired);
            } else {
                LOGGER.debugCr(reconciliation, "{} {}/{} already exists, updating it", resourceKind, namespace, name);
                return internalUpdate(reconciliation, namespace, name, current, desired);
            }
        } else {
            if (current != null) {
                // Deletion is desired
                LOGGER.debugCr(reconciliation, "{} {}/{} exist, deleting it", resourceKind, namespace, name);
                return internalDelete(reconciliation, namespace, name);
            } else {
                LOGGER.debugCr(reconciliation, "{} {}/{} does not exist, noop", resourceKind, namespace, name);
                return CompletableFuture.completedStage(ReconcileResult.noop(null));
            }
        }
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
     * @return CompletionStage which completes when the lists are reconciled
     */
    public CompletionStage<Void> batchReconcile(Reconciliation reconciliation, String namespace, List<T> desired, Labels selector)  {
        return listAsync(namespace, selector)
                .thenCompose(current -> {
                    List<CompletionStage<?>> futures = new ArrayList<>(desired.size());
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

                    return CompletableFuture.allOf(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
                });
    }

    /**
     * Deletes the resource with the given namespace and name and completes the
     * given CompletionStage accordingly. This method will do a cascading delete.
     *
     * @param reconciliation The reconciliation
     * @param namespace      Namespace of the resource which should be deleted
     * @param name           Name of the resource which should be deleted
     *
     * @return A CompletionStage which will be completed on the context thread once
     *         the resource has been deleted.
     */
    protected CompletionStage<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String namespace, String name) {
        return internalDelete(reconciliation, namespace, name, true);
    }

    /**
     * Asynchronously deletes the resource in the given {@code namespace} with the
     * given {@code name}, returning a CompletionStage which completes once the
     * resource is observed to have been deleted.
     *
     * @param reconciliation The reconciliation
     * @param namespace      Namespace of the resource which should be deleted
     * @param name           Name of the resource which should be deleted
     * @param cascading      Defines whether the deletion should be cascading or not
     *                       (e.g. whether an STS deletion should delete pods etc.)
     *
     * @return A CompletionStage which will be completed on the context thread once
     *         the resource has been deleted.
     */
    protected CompletionStage<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        R resourceOp = operation().inNamespace(namespace).withName(name);

        CompletableFuture<?> watchForDeleteFuture = resourceSupport.selfClosingWatch(
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
            })
            .toCompletableFuture();

        CompletableFuture<?> deleteFuture = resourceSupport.deleteAsync(resourceOp.withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L))
                .toCompletableFuture();

        return CompletableFuture.allOf(watchForDeleteFuture, deleteFuture)
            .thenApply(nothing -> ReconcileResult.deleted());
    }

    /**
     * Patches the resource with the given namespace and name to match the given desired resource
     * and completes the given future accordingly.
     */
    protected CompletionStage<ReconcileResult<T>> internalUpdate(Reconciliation reconciliation, String namespace, String name, T current, T desired) {
        if (needsPatching(reconciliation, name, current, desired)) {
            return patchOrReplace(namespace, name, desired)
                .thenApply(result -> wasChanged(current, result) ? ReconcileResult.patched(result) : ReconcileResult.noop(result))
                .whenComplete((result, error) -> {
                    if (error == null) {
                        LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been patched", resourceKind, name, namespace);
                    } else {
                        LOGGER.debugCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, error);
                    }
                });
        } else {
            LOGGER.debugCr(reconciliation, "{} {} in namespace {} did not changed and doesn't need patching", resourceKind, name, namespace);
            return CompletableFuture.completedStage(ReconcileResult.noop(current));
        }
    }

    /**
     * Method for patching or replacing a resource. By default, is using JSON-type patch. Overriding this method can be
     * used to use replace instead of patch or different patch strategies.
     *
     * @param namespace     Namespace of the resource
     * @param name          Name of the resource
     * @param desired       Desired resource
     *
     * @return  The patched or replaced resource
     */
    protected CompletionStage<T> patchOrReplace(String namespace, String name, T desired) {
        return CompletableFuture.supplyAsync(
                () -> operation().inNamespace(namespace).withName(name).patch(PatchContext.of(PatchType.JSON), desired),
                asyncExecutor);
    }

    /**
     * Creates a resource with the given namespace and name with the given desired
     * state and completes the given future accordingly. If the resource already
     * exists (Kubernetes responds with 409/Conflict), then an attempt will be made
     * to update the existing resource with the desired state.
     */
    protected CompletionStage<ReconcileResult<T>> internalCreate(Reconciliation reconciliation, String namespace, String name, T desired) {
        return CompletableFuture.supplyAsync(() -> {
            R resource = operation().inNamespace(namespace).resource(desired);
            T result;

            try {
                result = resource.create();
            } catch (KubernetesClientException e) {
                if (e.getCode() == 409) {
                    LOGGER.debugCr(reconciliation, "{} {} in namespace {} already exists and cannot be created. It will be updated instead", resourceKind, name, namespace);
                    result = resource.update();
                } else {
                    throw e;
                }
            }

            return result;
        }, asyncExecutor)
            .thenApply(ReconcileResult::created)
            .whenComplete((result, error) -> {
                if (error == null) {
                    LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been created", resourceKind, name, namespace);
                } else {
                    LOGGER.debugCr(reconciliation, "Caught exception while creating {} {} in namespace {}", resourceKind, name, namespace, error);
                }
            });
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
     * Asynchronously gets the resource with the given {@code name} in the given
     * {@code namespace}.
     *
     * @param namespace The namespace.
     * @param name      The name.
     * @return A CompletionStage for the result.
     */
    public CompletionStage<T> getAsync(String namespace, String name) {
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
    private FilterWatchListDeletable<T, L, R> applyNamespace(String namespace) {
        if (ConfigParameter.ANY_NAMESPACE.equals(namespace)) {
            return operation().inAnyNamespace();
        } else {
            return operation().inNamespace(namespace);
        }
    }

    /**
     * Asynchronously lists the resource with the given {@code selector} in the
     * given {@code namespace}.
     *
     * @param namespace The namespace.
     * @param selector  The selector.
     * @return A CompletionStage with a list of matching resources.
     */
    public CompletionStage<List<T>> listAsync(String namespace, Labels selector) {
        return listAsync(applySelector(applyNamespace(namespace), selector));
    }

    /**
     * Asynchronously lists the resource with the given {@code selector} in the
     * given {@code namespace}.
     *
     * @param namespace Namespace where the resources should be listed
     * @param selector  Label selector for selecting only some of the resources
     *
     * @return A CompletionStage with a list of matching resources.
     */
    public CompletionStage<List<T>> listAsync(String namespace, LabelSelector selector) {
        return listAsync(applySelector(applyNamespace(namespace), selector));
    }

    /**
     * Returns a CompletionStage that completes when the resource identified by the
     * given {@code namespace} and {@code name} is ready.
     *
     * @param reconciliation The reconciliation
     * @param namespace      The namespace.
     * @param name           The resource name.
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs      The timeout, in milliseconds.
     * @param predicate      The predicate.
     * @return A CompletionStage that completes when the resource identified by the
     *         given {@code namespace} and {@code name} is ready.
     */
    public CompletionStage<Void> waitFor(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
        return waitFor(reconciliation, namespace, name, "ready", pollIntervalMs, timeoutMs, predicate);
    }

    /**
     * Returns a CompletionStage that completes when the resource identified by the
     * given {@code namespace} and {@code name} is ready.
     *
     * @param reconciliation The reconciliation
     * @param namespace      The namespace.
     * @param name           The resource name.
     * @param logState       The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs      The timeout, in milliseconds.
     * @param predicate      The predicate.
     * @return A CompletionStage that completes when the resource identified by the
     *         given {@code namespace} and {@code name} is ready.
     */
    public CompletionStage<Void> waitFor(Reconciliation reconciliation, String namespace, String name, String logState, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
        return resourceSupport.waitFor(reconciliation,
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
    public CompletionStage<Void> deleteAsync(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        return internalDelete(reconciliation, namespace, name, cascading).thenRun(ResourceSupport.NOOP);
    }

    /**
     * Creates the informer for given resource type to inform on all instances in given namespace (or cluster-wide). The
     * informer returned by this method is not running and has to be started by the code using it.
     *
     * @param namespace         Namespace on which to inform
     * @param resyncIntervalMs  The interval in which the resync of the informer should happen in milliseconds
     *
     * @return          Informer instance
     */
    public SharedIndexInformer<T> informer(String namespace, long resyncIntervalMs) {
        return runnableInformer(applyNamespace(namespace), resyncIntervalMs);
    }

    /**
     * Creates the informer for given resource type to inform on all instances in given namespace (or cluster-wide)
     * matching the selector. The informer returned by this method is not running and has to be started by the code
     * using it.
     *
     * @param namespace         Namespace on which to inform
     * @param selectorLabels    Selector which should be matched by the resources
     * @param resyncIntervalMs  The interval in which the resync of the informer should happen in milliseconds
     *
     * @return                  Informer instance
     */
    public SharedIndexInformer<T> informer(String namespace, Map<String, String> selectorLabels, long resyncIntervalMs) {
        return runnableInformer(applyNamespace(namespace).withLabels(selectorLabels), resyncIntervalMs);
    }

    /**
     * Creates the informer for given resource type to inform on all instances in given namespace (or cluster-wide)
     * matching the selector. The informer returned by this method is not running and has to be started by the code
     * using it.
     *
     * @param namespace         Namespace on which to inform
     * @param labelSelector     Labels Selector which should be matched by the resources
     * @param resyncIntervalMs  The interval in which the resync of the informer should happen in milliseconds
     *
     * @return                  Informer instance
     */
    public SharedIndexInformer<T> informer(String namespace, LabelSelector labelSelector, long resyncIntervalMs) {
        return runnableInformer(applyNamespace(namespace).withLabelSelector(labelSelector), resyncIntervalMs);
    }

    /**
     * Creates a runnable informer. Runnable informer is not running yet and need to be started by the code using it.
     *
     * @param informable        Instance of the Informable interface for creating informers
     * @param resyncIntervalMs  The interval in which the resync of the informer should happen in milliseconds
     *
     * @return  Runnable informer
     */
    private SharedIndexInformer<T> runnableInformer(Informable<T> informable, long resyncIntervalMs) {
        return informable.runnableInformer(resyncIntervalMs);
    }

    /**
     * Returns the Kubernetes client for given resource type
     *
     * @return  Kubernetes client instance for given resource
     */
    public MixedOperation<T, L, R> client() {
        return operation();
    }

    /**
     * Returns a Kubernetes client {@linkplain Resource} of this resource
     * operator's type T for the given resource namespace and name.
     *
     * @param namespace namespace of the resource
     * @param name name of the resource
     * @return Kubernetes client {@linkplain Resource}
     */
    public Resource<T> resource(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name);
    }

    /**
     * Returns a Kubernetes client {@linkplain Resource} of this resource
     * operator's type T for the given resource namespace and item.
     *
     * @param namespace namespace of the resource
     * @param item instance of the resource
     * @return Kubernetes client {@linkplain Resource}
     */
    public Resource<T> resource(String namespace, T item) {
        return operation().inNamespace(namespace).resource(item);
    }
}
