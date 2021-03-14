/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Operations for {@code StatefulSets}s, which supports {@link #maybeRollingUpdate(StatefulSet, Function)}
 * in addition to the usual operations.
 */
public abstract class StatefulSetOperator extends AbstractScalableResourceOperator<KubernetesClient, StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> {

    private static final int NO_GENERATION = -1;
    private static final int INIT_GENERATION = 0;

    private static final Logger log = LogManager.getLogger(StatefulSetOperator.class.getName());
    protected final PodOperator podOperations;
    private final PvcOperator pvcOperations;
    protected final long operationTimeoutMs;
    protected final SecretOperator secretOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance.
     * @param client The Kubernetes client.
     * @param operationTimeoutMs The timeout.
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        this(vertx, client, operationTimeoutMs, new PodOperator(vertx, client), new PvcOperator(vertx, client));
    }

    /**
     * @param vertx The Vertx instance.
     * @param client The Kubernetes client.
     * @param operationTimeoutMs The timeout.
     * @param podOperator The pod operator.
     * @param pvcOperator The PVC operator.
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs,
                               PodOperator podOperator, PvcOperator pvcOperator) {
        super(vertx, client, "StatefulSet");
        this.secretOperations = new SecretOperator(vertx, client);
        this.podOperations = podOperator;
        this.operationTimeoutMs = operationTimeoutMs;
        this.pvcOperations = pvcOperator;
    }

    @Override
    protected MixedOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> operation() {
        return client.apps().statefulSets();
    }

    /**
     * Asynchronously perform a rolling update of all the pods in the StatefulSet identified by the given
     * {@code namespace} and {@code name}, returning a Future that will complete when the rolling update
     * is complete. Starting with pod 0, each pod will be deleted and re-created automatically by the ReplicaSet,
     * once the pod has been recreated then given {@code isReady} function will be polled until it returns true,
     * before the process proceeds with the pod with the next higher number.
     * @param sts The StatefulSet
     * @param podNeedsRestart Function that returns a list is reasons why the given pod needs to be restarted, or an empty list if the pod does not need to be restarted.
     * @return A future that completes when any necessary rolling has been completed.
     */
    public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
        return getSecrets(sts).compose(compositeFuture -> {
            return maybeRollingUpdate(sts, podNeedsRestart, compositeFuture.resultAt(0), compositeFuture.resultAt(1));
        });
    }

    protected CompositeFuture getSecrets(StatefulSet sts) {
        String cluster = sts.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String namespace = sts.getMetadata().getNamespace();
        Future<Secret> clusterCaCertSecretFuture = secretOperations.getAsync(
            namespace, KafkaResources.clusterCaCertificateSecretName(cluster)).compose(secret -> {
                if (secret == null) {
                    return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaCertSecretName(cluster)));
                } else {
                    return Future.succeededFuture(secret);
                }
            });
        Future<Secret> coKeySecretFuture = secretOperations.getAsync(
            namespace, ClusterOperator.secretName(cluster)).compose(secret -> {
                if (secret == null) {
                    return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(cluster)));
                } else {
                    return Future.succeededFuture(secret);
                }
            });
        return CompositeFuture.join(clusterCaCertSecretFuture, coKeySecretFuture);
    }

    public abstract Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret);

    public Future<Void> deletePvc(StatefulSet sts, String pvcName) {
        String namespace = sts.getMetadata().getNamespace();
        Promise<Void> promise = Promise.promise();
        Future<ReconcileResult<PersistentVolumeClaim>> r = pvcOperations.reconcile(namespace, pvcName, null);
        r.onComplete(h -> {
            if (h.succeeded()) {
                promise.complete();
            } else {
                promise.fail(h.cause());
            }
        });
        return promise.future();
    }

    /**
     * Asynchronously apply the given {@code podNeedsRestart}, if it returns true then restart the pod
     * given by {@code podName} by deleting it and letting it be recreated by K8s;
     * in any case return a Future which completes when the given (possibly recreated) pod is ready.
     * @param sts The StatefulSet.
     * @param podName The name of the Pod to possibly restart.
     * @param podNeedsRestart The function for deciding whether to restart the pod.
     * @return a Future which completes when the given (possibly recreated) pod is ready.
     */
    Future<Void> maybeRestartPod(StatefulSet sts, String podName, Function<Pod, List<String>> podNeedsRestart) {
        long pollingIntervalMs = 1_000;
        long timeoutMs = operationTimeoutMs;
        String namespace = sts.getMetadata().getNamespace();
        String name = sts.getMetadata().getName();
        return podOperations.getAsync(sts.getMetadata().getNamespace(), podName).compose(pod -> {
            Future<Void> fut;
            List<String> reasons = podNeedsRestart.apply(pod);
            if (reasons != null && !reasons.isEmpty()) {
                log.debug("Rolling update of {}/{}: pod {} due to {}", namespace, name, podName, reasons);
                fut = restartPod(sts, pod);
            } else {
                log.debug("Rolling update of {}/{}: pod {} no need to roll", namespace, name, podName);
                fut = Future.succeededFuture();
            }
            return fut.compose(ignored -> {
                log.debug("Rolling update of {}/{}: wait for pod {} readiness", namespace, name, podName);
                return podOperations.readiness(namespace, podName, pollingIntervalMs, timeoutMs);
            });
        });
    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be ready when the returned Future completes.
     * @param sts The StatefulSet
     * @param pod The pod to be restarted
     * @return a Future which completes when the Pod has been recreated
     */
    private Future<Void> restartPod(StatefulSet sts, Pod pod) {
        return podOperations.restart("Rolling update of " + sts.getMetadata().getNamespace() + "/" + sts.getMetadata().getName(),
                pod, operationTimeoutMs);
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

    private static ObjectMeta templateMetadata(StatefulSet resource) {
        return resource.getSpec().getTemplate().getMetadata();
    }

    /**
     * The name of the given pod given by {@code podId} in the given StatefulSet.
     * @param desired The StatefulSet
     * @param podId The pod id.
     * @return The name of the pod.
     */
    public String getPodName(StatefulSet desired, int podId) {
        return templateMetadata(desired).getName() + "-" + podId;
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

    protected abstract boolean shouldIncrementGeneration(StatefulSetDiff diff);

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
    protected Future<ReconcileResult<StatefulSet>> internalCreate(String namespace, String name, StatefulSet desired) {
        // Create the STS...
        Promise<ReconcileResult<StatefulSet>> result = Promise.promise();
        setGeneration(desired, INIT_GENERATION);
        Future<ReconcileResult<StatefulSet>> crt = super.internalCreate(namespace, name, desired);

        if (crt.failed()) {
            return crt;
        }
        // ... then wait for the STS to be ready...
        crt.compose(res -> readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs).map(res))
        // ... then wait for all the pods to be ready
            .compose(res -> podReadiness(namespace, desired, 1_000, operationTimeoutMs).map(res))
            .onComplete(result);

        return result.future();
    }

    /**
     * Returns a future that completes when all the pods [0..replicas-1] in the given statefulSet are ready.
     */
    protected Future<?> podReadiness(String namespace, StatefulSet desired, long pollInterval, long operationTimeoutMs) {
        final int replicas = desired.getSpec().getReplicas();
        List<Future> waitPodResult = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            String podName = getPodName(desired, i);
            waitPodResult.add(podOperations.readiness(namespace, podName, pollInterval, operationTimeoutMs));
        }
        return CompositeFuture.join(waitPodResult);
    }

    /**
     * Overridden to not cascade to dependent resources (e.g. pods).
     *
     * {@inheritDoc}
     */
    @Override
    protected Future<ReconcileResult<StatefulSet>> internalPatch(String namespace, String name, StatefulSet current, StatefulSet desired) {
        StatefulSetDiff diff = new StatefulSetDiff(current, desired);

        if (shouldIncrementGeneration(diff)) {
            incrementGeneration(current, desired);
        } else {
            setGeneration(desired, getStsGeneration(current));
        }

        // Don't scale via patch
        desired.getSpec().setReplicas(current.getSpec().getReplicas());
        if (log.isTraceEnabled()) {
            log.trace("Patching {} {}/{} to match desired state {}", resourceKind, namespace, name, desired);
        } else {
            log.debug("Patching {} {}/{}", resourceKind, namespace, name);
        }

        if (diff.changesVolumeClaimTemplates() || diff.changesVolumeSize()) {
            // When volume claim templates change, we need to delete the STS and re-create it
            return internalReplace(namespace, name, current, desired, false);
        } else {
            return super.internalPatch(namespace, name, current, desired, false);
        }

    }

    /**
     * Sometimes, patching the resource is not enough. For example when the persistent volume claim templates are modified.
     * In such case we need to delete the STS with cascading=false and recreate it.
     * A rolling update should done finished after the STS is recreated.
     *
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param current Current StatefulSet
     * @param desired Desired StatefulSet
     * @param cascading Defines whether the delete should be cascading or not (e.g. whether a STS deletion should delete pods etc.)
     *
     * @return Future with result of the reconciliation
     */
    protected Future<ReconcileResult<StatefulSet>> internalReplace(String namespace, String name, StatefulSet current, StatefulSet desired, boolean cascading) {
        try {
            Promise<ReconcileResult<StatefulSet>> promise = Promise.promise();

            long pollingIntervalMs = 1_000;
            long timeoutMs = operationTimeoutMs;

            operation().inNamespace(namespace).withName(name).withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L).delete();

            Future<Void> deletedFut = waitFor(namespace, name, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                StatefulSet sts = get(namespace, name);
                log.trace("Checking if {} {} in namespace {} has been deleted", resourceKind, name, namespace);
                return sts == null;
            });

            deletedFut.onComplete(res -> {
                if (res.succeeded())    {
                    StatefulSet result = operation().inNamespace(namespace).withName(name).create(desired);
                    log.debug("{} {} in namespace {} has been replaced", resourceKind, name, namespace);
                    promise.complete(wasChanged(current, result) ? ReconcileResult.patched(result) : ReconcileResult.noop(result));
                } else {
                    promise.fail(res.cause());
                }
            });

            return promise.future();
        } catch (Exception e) {
            log.debug("Caught exception while replacing {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    /**
     * Asynchronously deletes the resource with the given {@code name} in the given {@code namespace}.
     *
     * @param namespace Namespace of the resource which should be deleted
     * @param name Name of the resource which should be deleted
     * @param cascading Defines whether the deletion should be cascading or not
     *
     * @return A Future with True if the deletion succeeded and False when it failed.
     */
    public Future<Void> deleteAsync(String namespace, String name, boolean cascading) {
        Promise<Void> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                try {
                    Boolean deleted = operation().inNamespace(namespace).withName(name).withPropagationPolicy(cascading ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN).withGracePeriod(-1L).delete();

                    if (deleted) {
                        log.debug("{} {} in namespace {} has been deleted", resourceKind, name, namespace);
                        future.complete();
                    } else  {
                        log.debug("{} {} in namespace {} has been not been deleted", resourceKind, name, namespace);
                        future.fail(resourceKind + " " + name + " in namespace " + namespace + " has been not been deleted");
                    }
                } catch (Exception e) {
                    log.debug("Caught exception while deleting {} {} in namespace {}", resourceKind, name, namespace, e);
                    future.fail(e);
                }
            }, true, result
        );
        return result.future();
    }
}
