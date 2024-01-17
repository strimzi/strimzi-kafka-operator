/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Operator for managing CRD resources
 *
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The custom resource type.
 * @param <L> The list variant of the custom resource type.
 */
public class CrdOperator<C extends KubernetesClient,
            T extends CustomResource<?, ?>,
            L extends DefaultKubernetesResourceList<T>>
        extends AbstractWatchableStatusedNamespacedResourceOperator<C, T, L, Resource<T>> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CrdOperator.class);

    private final Class<T> cls;
    private final Class<L> listCls;

    /**
     * Constructor
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client        The Kubernetes client
     * @param cls           The class of the CR
     * @param listCls       The class of the list.
     * @param kind          The Kind of the CR for which this operator should be
     *                      used
     */
    public CrdOperator(Executor asyncExecutor, C client, Class<T> cls, Class<L> listCls, String kind) {
        super(asyncExecutor, client, kind);
        this.cls = cls;
        this.listCls = listCls;
    }

    @Override
    protected MixedOperation<T, L, Resource<T>> operation() {
        return client.resources(cls, listCls);
    }

    /**
     * The selfClosingWatch does not work for Custom Resources. Therefore we override the method and delete custom
     * resources without it.
     *
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param cascading Defines whether the delete should be cascading or not (e.g. whether a STS deletion should delete pods etc.)
     *
     * @return A future which will be completed on the context thread
     *         once the resource has been deleted.
     */
    @Override
    protected CompletionStage<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        Resource<T> resourceOp = operation().inNamespace(namespace).withName(name);

        CompletableFuture<Void> watchForDeleteFuture = resourceSupport.waitFor(reconciliation,
            String.format("%s resource %s", resourceKind, name),
            "deleted",
            1_000,
            deleteTimeoutMs(),
            () -> resourceOp.get() == null)
            .toCompletableFuture();

        CompletableFuture<Void> deleteFuture = resourceSupport.deleteAsync(resourceOp.withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L))
                .toCompletableFuture();

        return CompletableFuture.allOf(watchForDeleteFuture, deleteFuture)
                .thenApply(nothing -> ReconcileResult.deleted());
    }

    /**
     * Patches custom resource asynchronously
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Desired resource
     *
     * @return  Future which completes when the resource is patched
     */
    public CompletionStage<T> patchAsync(Reconciliation reconciliation, T resource) {
        String namespace = resource.getMetadata().getNamespace();
        String name = resource.getMetadata().getName();

        return CompletableFuture.supplyAsync(
                () -> operation()
                    .inNamespace(namespace)
                    .withName(name)
                    .patch(PatchContext.of(PatchType.JSON), resource),
                asyncExecutor)
            .whenComplete((result, error) -> {
                if (error == null) {
                    LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been patched", resourceKind, name, namespace);
                } else {
                    LOGGER.debugCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, error);
                }
            });
    }

    /**
     * Updates custom resource status asynchronously
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Desired resource with the updated status
     *
     * @return  Future which completes when the status is patched
     */
    public CompletionStage<T> updateStatusAsync(Reconciliation reconciliation, T resource) {
        String namespace = resource.getMetadata().getNamespace();
        String name = resource.getMetadata().getName();

        return CompletableFuture.supplyAsync(
                operation().inNamespace(namespace).resource(resource)::updateStatus,
                asyncExecutor)
            .whenComplete((result, error) -> {
                if (error == null) {
                    LOGGER.infoCr(reconciliation, "Status of {} {} in namespace {} has been updated", resourceKind, name, namespace);
                } else {
                    LOGGER.debugCr(reconciliation, "Caught exception while updating status of {} {} in namespace {}", resourceKind, name, namespace, error);
                }
            });
    }
}
