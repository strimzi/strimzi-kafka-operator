/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.AbstractScalableNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Operations for {@code StatefulSets}s
 */
public class StatefulSetOperator extends AbstractScalableNamespacedResourceOperator<KubernetesClient, StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> {
    private static final int NO_GENERATION = -1;
    private static final int INIT_GENERATION = 0;

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(StatefulSetOperator.class.getName());
    protected final PodOperator podOperations;
    protected final long operationTimeoutMs;

    /**
     * Constructor
     * @param vertx The Vertx instance.
     * @param client The Kubernetes client.
     * @param operationTimeoutMs The timeout.
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        this(vertx, client, operationTimeoutMs, new PodOperator(vertx, client));
    }

    /**
     * @param vertx The Vertx instance.
     * @param client The Kubernetes client.
     * @param operationTimeoutMs The timeout.
     * @param podOperator The pod operator.
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs, PodOperator podOperator) {
        super(vertx, client, "StatefulSet");
        this.podOperations = podOperator;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    @Override
    protected MixedOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> operation() {
        return client.apps().statefulSets();
    }

    @Override
    protected Integer currentScale(String namespace, String name) {
        StatefulSet statefulSet = get(namespace, name);
        if (statefulSet != null) {
            return statefulSet.getSpec().getReplicas();
        } else {
            return null;
        }
    }

    /**
     * The name of the given pod given by {@code podId} in the given StatefulSet.
     * @param desired The StatefulSet
     * @param podId The pod id.
     * @return The name of the pod.
     */
    public String getPodName(StatefulSet desired, int podId) {
        return desired.getMetadata().getName() + "-" + podId;
    }

    private void setGeneration(StatefulSet desired, int nextGeneration) {
        Map<String, String> annotations = Annotations.annotations(desired.getSpec().getTemplate());
        annotations.put(ANNO_STRIMZI_IO_GENERATION, String.valueOf(nextGeneration));
    }

    protected void incrementGeneration(StatefulSet current, StatefulSet desired) {
        final int generation = Annotations.intAnnotation(current.getSpec().getTemplate(), ANNO_STRIMZI_IO_GENERATION, INIT_GENERATION);
        final int nextGeneration = generation + 1;
        setGeneration(desired, nextGeneration);
    }

    protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
        return !diff.isEmpty() && needsRollingUpdate(reconciliation, diff);
    }

    /**
     * Checks if rolling update is needed or not
     *
     * @param reconciliation    Reconciliation marker
     * @param diff              StatefulSet diff
     *
     * @return  True if restart is needed. False otherwise.
     */
    public static boolean needsRollingUpdate(Reconciliation reconciliation, StatefulSetDiff diff) {
        if (diff.changesLabels()) {
            LOGGER.debugCr(reconciliation, "Changed labels => needs rolling update");
            return true;
        }

        if (diff.changesSpecTemplate()) {
            LOGGER.debugCr(reconciliation, "Changed template spec => needs rolling update");
            return true;
        }

        if (diff.changesVolumeClaimTemplates()) {
            LOGGER.debugCr(reconciliation, "Changed volume claim template => needs rolling update");
            return true;
        }

        if (diff.changesVolumeSize()) {
            LOGGER.debugCr(reconciliation, "Changed size of the volume claim template => no need for rolling update");
            return false;
        }

        return false;
    }

    /**
     * Gets the {@code strimzi.io/generation} of the given StatefulSet.
     * @param resource the StatefulSet.
     * @return The {@code strimzi.io/generation} of the given StatefulSet.
     */
    public static int getStsGeneration(StatefulSet resource) {
        if (resource == null) {
            return NO_GENERATION;
        }
        return Annotations.intAnnotation(resource.getSpec().getTemplate(), ANNO_STRIMZI_IO_GENERATION, NO_GENERATION);
    }

    /**
     * Gets the {@code strimzi.io/generation} of the given Pod
     * @param resource the Pod.
     * @return The {@code strimzi.io/generation} of the given Pod.
     */
    public static int getPodGeneration(Pod resource) {
        if (resource == null) {
            return NO_GENERATION;
        }
        return Annotations.intAnnotation(resource, ANNO_STRIMZI_IO_GENERATION, NO_GENERATION);
    }

    @Override
    protected Future<ReconcileResult<StatefulSet>> internalCreate(Reconciliation reconciliation, String namespace, String name, StatefulSet desired) {
        // Create the STS...
        Promise<ReconcileResult<StatefulSet>> result = Promise.promise();
        setGeneration(desired, INIT_GENERATION);
        Future<ReconcileResult<StatefulSet>> crt = super.internalCreate(reconciliation, namespace, name, desired);

        if (crt.failed()) {
            return crt;
        }
        // ... then wait for the STS to be ready...
        crt.compose(res -> readiness(reconciliation, namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs).map(res))
        // ... then wait for all the pods to be ready
            .compose(res -> podReadiness(reconciliation, namespace, desired, 1_000, operationTimeoutMs).map(res))
            .onComplete(result);

        return result.future();
    }

    /**
     * Returns a future that completes when all the pods [0...replicas-1] in the given statefulSet are ready.
     */
    protected Future<?> podReadiness(Reconciliation reconciliation, String namespace, StatefulSet desired, long pollInterval, long operationTimeoutMs) {
        final int replicas = desired.getSpec().getReplicas();
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> waitPodResult = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            String podName = getPodName(desired, i);
            waitPodResult.add(podOperations.readiness(reconciliation, namespace, podName, pollInterval, operationTimeoutMs));
        }
        return CompositeFuture.join(waitPodResult);
    }

    /**
     * Overridden to not cascade to dependent resources (e.g. pods).
     *
     * {@inheritDoc}
     */
    @Override
    protected Future<ReconcileResult<StatefulSet>> internalPatch(Reconciliation reconciliation, String namespace, String name, StatefulSet current, StatefulSet desired) {
        StatefulSetDiff diff = new StatefulSetDiff(reconciliation, current, desired);

        if (shouldIncrementGeneration(reconciliation, diff)) {
            incrementGeneration(current, desired);
        } else {
            setGeneration(desired, getStsGeneration(current));
        }

        // Don't scale via patch
        desired.getSpec().setReplicas(current.getSpec().getReplicas());
        LOGGER.traceCr(reconciliation, "Patching {} {}/{} to match desired state {}", resourceKind, namespace, name, desired);
        LOGGER.debugCr(reconciliation, "Patching {} {}/{}", resourceKind, namespace, name);

        if (diff.changesVolumeClaimTemplates() || diff.changesVolumeSize()) {
            // When volume claim templates change, we need to delete the STS and re-create it
            return internalReplace(reconciliation, namespace, name, current, desired, false);
        } else {
            return super.internalPatch(reconciliation, namespace, name, current, desired);
        }

    }

    /**
     * Sometimes, patching the resource is not enough. For example when the persistent volume claim templates are modified.
     * In such case we need to delete the STS with cascading=false and recreate it.
     * A rolling update should be finished after the STS is recreated.
     *
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param current Current StatefulSet
     * @param desired Desired StatefulSet
     * @param cascading Defines whether the deletion should be cascading or not (e.g. whether the STS deletion should delete pods etc.)
     *
     * @return Future with result of the reconciliation
     */
    protected Future<ReconcileResult<StatefulSet>> internalReplace(Reconciliation reconciliation, String namespace, String name, StatefulSet current, StatefulSet desired, boolean cascading) {
        try {
            Promise<ReconcileResult<StatefulSet>> promise = Promise.promise();

            long pollingIntervalMs = 1_000;

            operation().inNamespace(namespace).withName(name).withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L).delete();

            Future<Void> deletedFut = waitFor(reconciliation, namespace, name, "deleted", pollingIntervalMs, operationTimeoutMs, (ignore1, ignore2) -> {
                StatefulSet sts = get(namespace, name);
                LOGGER.traceCr(reconciliation, "Checking if {} {} in namespace {} has been deleted", resourceKind, name, namespace);
                return sts == null;
            });

            deletedFut.onComplete(res -> {
                if (res.succeeded())    {
                    StatefulSet result = operation().inNamespace(namespace).resource(desired).create();
                    LOGGER.debugCr(reconciliation, "{} {} in namespace {} has been replaced", resourceKind, name, namespace);
                    promise.complete(wasChanged(current, result) ? ReconcileResult.patched(result) : ReconcileResult.noop(result));
                } else {
                    promise.fail(res.cause());
                }
            });

            return promise.future();
        } catch (Exception e) {
            LOGGER.debugCr(reconciliation, "Caught exception while replacing {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }
}
