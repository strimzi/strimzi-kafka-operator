/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import io.strimzi.operator.common.StrimziFuture;
import io.strimzi.operator.common.Util;

/**
 * Operator for managing CRD resources
 *
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The custom resource type.
 * @param <L> The list variant of the custom resource type.
 */
@SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
        justification = "Erroneous on Java 11: https://github.com/spotbugs/spotbugs/issues/756")
public class CrdOperator<C extends KubernetesClient,
            T extends CustomResource<?, ?>,
            L extends DefaultKubernetesResourceList<T>>
        extends AbstractWatchableStatusedNamespacedResourceOperator<C, T, L, Resource<T>> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CrdOperator.class);

    private final Class<T> cls;
    private final Class<L> listCls;

    /**
     * Constructor
     * @param client The Kubernetes client
     * @param cls The class of the CR
     * @param listCls The class of the list.
     * @param kind The Kind of the CR for which this operator should be used
     */
    public CrdOperator(C client, Class<T> cls, Class<L> listCls, String kind) {
        super(client, kind);
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
    protected StrimziFuture<ReconcileResult<T>> internalDelete(Reconciliation reconciliation, String namespace, String name, boolean cascading) {
        Resource<T> resourceOp = operation().inNamespace(namespace).withName(name);

        CompletionStage<Void> watchForDeleteFuture = Util.waitFor(reconciliation,
            String.format("%s resource %s", resourceKind, name),
            "deleted",
            1_000,
            deleteTimeoutMs(),
            () -> resourceOp.get() != null);

        CompletionStage<Void> deleteFuture = resourceSupport.deleteAsync(resourceOp.withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L));

        return StrimziFuture.allOf(watchForDeleteFuture, deleteFuture)
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
    public StrimziFuture<T> patchAsync(Reconciliation reconciliation, T resource) {
        StrimziFuture<T> blockingPromise = new StrimziFuture<>();

        StrimziFuture.runAsync(() -> {
            String namespace = resource.getMetadata().getNamespace();
            String name = resource.getMetadata().getName();

            try {
                T result = operation().inNamespace(namespace).withName(name).patch(PatchContext.of(PatchType.JSON), resource);
                LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been patched", resourceKind, name, namespace);
                blockingPromise.complete(result);
            } catch (Exception e) {
                LOGGER.debugCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
                blockingPromise.completeExceptionally(e);
            }
        });

        return blockingPromise;
    }

    /**
     * Updates custom resource status asynchronously
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Desired resource with the updated statis
     *
     * @return  Future which completes when the status is patched
     */
    public StrimziFuture<T> updateStatusAsync(Reconciliation reconciliation, T resource) {
        StrimziFuture<T> blockingPromise = new StrimziFuture<>();

        StrimziFuture.runAsync(() -> {
            String namespace = resource.getMetadata().getNamespace();
            String name = resource.getMetadata().getName();

            try {
                T result = operation().inNamespace(namespace).resource(resource).updateStatus();
                LOGGER.infoCr(reconciliation, "Status of {} {} in namespace {} has been updated", resourceKind, name, namespace);
                blockingPromise.complete(result);
            } catch (Exception e) {
                LOGGER.debugCr(reconciliation, "Caught exception while updating status of {} {} in namespace {}", resourceKind, name, namespace, e);
                blockingPromise.completeExceptionally(e);
            }
        });

        return blockingPromise;
    }
}
