/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Operations for {@code StatefulSets}s, which supports {@link #maybeRollingUpdate(StatefulSet, Predicate)}
 * in addition to the usual operations.
 */
public abstract class StatefulSetOperator extends AbstractScalableResourceOperator<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> {

    private static final int NO_GENERATION = -1;
    private static final String NO_UID = "NULL";
    private static final int INIT_GENERATION = 0;

    private static final Logger log = LogManager.getLogger(StatefulSetOperator.class.getName());
    private final PodOperator podOperations;
    private final PvcOperator pvcOperations;
    protected final long operationTimeoutMs;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        this(vertx, client, operationTimeoutMs, new PodOperator(vertx, client), new PvcOperator(vertx, client));
    }

    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs, PodOperator podOperator, PvcOperator pvcOperator) {
        super(vertx, client, "StatefulSet");
        this.podOperations = podOperator;
        this.operationTimeoutMs = operationTimeoutMs;
        this.pvcOperations = pvcOperator;
    }

    @Override
    protected MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> operation() {
        return client.apps().statefulSets();
    }

    /**
     * Asynchronously perform a rolling update of all the pods in the StatefulSet identified by the given
     * {@code namespace} and {@code name}, returning a Future that will complete when the rolling update
     * is complete. Starting with pod 0, each pod will be deleted and re-created automatically by the ReplicaSet,
     * once the pod has been recreated then given {@code isReady} function will be polled until it returns true,
     * before the process proceeds with the pod with the next higher number.
     */
    public Future<Void> maybeRollingUpdate(StatefulSet ss, Predicate<Pod> podRestart) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        final int replicas = ss.getSpec().getReplicas();
        log.debug("Considering rolling update of {}/{}", namespace, name);
        Future<Void> f = Future.succeededFuture();
        // Then for each replica, maybe restart it
        for (int i = 0; i < replicas; i++) {
            String podName = name + "-" + i;
            f = f.compose(ignored -> maybeRestartPod(ss, podName, podRestart));
        }
        return f;
    }

    public Future<Void> maybeDeletePodAndPvc(StatefulSet ss) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        final int replicas = ss.getSpec().getReplicas();
        log.debug("Considering manual deletion and restart of pods for {}/{}", namespace, name);
        Future<Void> f = Future.succeededFuture();
        for (int i = 0; i < replicas; i++) {
            String podName = name + "-" + i;
            String pvcName = AbstractModel.getPersistentVolumeClaimName(name, i);
            Pod pod = podOperations.get(namespace, podName);

            if (pod != null) {
                if (Annotations.booleanAnnotation(pod, ANNO_STRIMZI_IO_DELETE_POD_AND_PVC,
                        false, ANNO_OP_STRIMZI_IO_DELETE_POD_AND_PVC)) {
                    f = f.compose(ignored -> deletePvc(ss, pvcName))
                            .compose(ignored -> maybeRestartPod(ss, podName, p -> true));

                }
            }
        }
        return f;
    }

    public Future<Void> deletePvc(StatefulSet ss, String pvcName) {
        String namespace = ss.getMetadata().getNamespace();
        Future<Void> f = Future.future();
        Future<ReconcileResult<PersistentVolumeClaim>> r = pvcOperations.reconcile(namespace, pvcName, null);
        r.setHandler(h -> {
            if (h.succeeded()) {
                f.complete();
            } else {
                f.fail(h.cause());
            }
        });
        return f;
    }

    public Future<Void> maybeRestartPod(StatefulSet ss, String podName, Predicate<Pod> podRestart) {
        long pollingIntervalMs = 1_000;
        long timeoutMs = operationTimeoutMs;
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        Pod pod = podOperations.get(ss.getMetadata().getNamespace(), podName);
        if (podRestart.test(pod)) {
            Future<Void> result = Future.future();
            Future<ReconcileResult<Pod>> deleteFinished = Future.future();
            log.info("Rolling update of {}/{}: Rolling pod {}", namespace, name, podName);

            // Determine generation of deleted pod
            Future<String> deleted = getUid(namespace, podName);

            // Delete the pod
            Future<ReconcileResult<Pod>> podReconcileFuture = deleted.compose(l -> {
                log.debug("Rolling update of {}/{}: Waiting for pod {} to be deleted", namespace, name, podName);
                // null as desired parameter means pod will be deleted
                return podOperations.reconcile(namespace, podName, null);
            }).compose(ignore -> {
                Future del = podOperations.waitFor(namespace, name, pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                    // predicate - changed generation means pod has been updated
                    String newUid = getPodUid(podOperations.get(namespace, podName));
                    boolean done = !deleted.result().equals(newUid);
                    if (done) {
                        log.debug("Rolling pod {} finished", podName);
                    }
                    return done;
                });
                return del;
            });

            podReconcileFuture.setHandler(deleteResult -> {
                if (deleteResult.succeeded()) {
                    log.debug("Rolling update of {}/{}: Pod {} was deleted", namespace, name, podName);
                }
                deleteFinished.handle(deleteResult);
            });
            deleteFinished.compose(ix -> podOperations.readiness(namespace, podName, pollingIntervalMs, timeoutMs)).setHandler(result);
            return result;
        } else {
            log.debug("Rolling update of {}/{}: pod {} no need to roll", namespace, name, podName);
            return Future.succeededFuture();
        }
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

    public String getPodName(StatefulSet desired, int podId) {
        return templateMetadata(desired).getName() + "-" + podId;
    }

    private void setGeneration(StatefulSet desired, int nextGeneration) {
        Map<String, String> annotations = Annotations.annotations(desired.getSpec().getTemplate());
        annotations.remove(ANNO_OP_STRIMZI_IO_GENERATION);
        annotations.put(ANNO_STRIMZI_IO_GENERATION, String.valueOf(nextGeneration));
    }

    protected void incrementGeneration(StatefulSet current, StatefulSet desired) {
        final int generation = Annotations.intAnnotation(current.getSpec().getTemplate(), ANNO_STRIMZI_IO_GENERATION,
                INIT_GENERATION, ANNO_OP_STRIMZI_IO_GENERATION);
        final int nextGeneration = generation + 1;
        setGeneration(desired, nextGeneration);
    }

    protected abstract boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired);

    public static int getSsGeneration(StatefulSet resource) {
        if (resource == null) {
            return NO_GENERATION;
        }
        return Annotations.intAnnotation(resource.getSpec().getTemplate(), ANNO_STRIMZI_IO_GENERATION,
                NO_GENERATION, ANNO_OP_STRIMZI_IO_GENERATION);
    }

    public static int getPodGeneration(Pod resource) {
        if (resource == null) {
            return NO_GENERATION;
        }
        return Annotations.intAnnotation(resource, ANNO_STRIMZI_IO_GENERATION,
                NO_GENERATION, ANNO_OP_STRIMZI_IO_GENERATION);
    }

    @Override
    protected Future<ReconcileResult<StatefulSet>> internalCreate(String namespace, String name, StatefulSet desired) {
        // Create the SS...
        Future<ReconcileResult<StatefulSet>> result = Future.future();
        setGeneration(desired, INIT_GENERATION);
        Future<ReconcileResult<StatefulSet>> crt = super.internalCreate(namespace, name, desired);

        // ... then wait for the SS to be ready...
        crt.compose(res -> readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs).map(res))
        // ... then wait for all the pods to be ready
            .compose(res -> podReadiness(namespace, desired, 1_000, operationTimeoutMs).map(res))
            .compose(res -> result.complete(res), result);
        return result;
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
        if (shouldIncrementGeneration(current, desired)) {
            incrementGeneration(current, desired);
        } else {
            setGeneration(desired, getSsGeneration(current));
        }

        // Don't scale via patch
        desired.getSpec().setReplicas(current.getSpec().getReplicas());
        if (log.isTraceEnabled()) {
            log.trace("Patching {} {}/{} to match desired state {}", resourceKind, namespace, name, desired);
        } else {
            log.debug("Patching {} {}/{}", resourceKind, namespace, name);
        }
        return super.internalPatch(namespace, name, current, desired, false);
    }

    /**
     * Reverts the changes done storage configuration of running cluster. Such changes are not allowed.
     *
     * @param current Current StatefulSet
     * @param desired New StatefulSet
     *
     * @return Updated StatefulSetDiff after the storage patching
     */
    protected StatefulSetDiff revertStorageChanges(StatefulSet current, StatefulSet desired) {
        desired.getSpec().setVolumeClaimTemplates(current.getSpec().getVolumeClaimTemplates());
        desired.getSpec().getTemplate().getSpec().setInitContainers(current.getSpec().getTemplate().getSpec().getInitContainers());
        desired.getSpec().getTemplate().getSpec().setSecurityContext(current.getSpec().getTemplate().getSpec().getSecurityContext());

        if (current.getSpec().getVolumeClaimTemplates().isEmpty()) {
            // We are on ephemeral storage and changing to persistent
            List<Volume> volumes = current.getSpec().getTemplate().getSpec().getVolumes();
            for (int i = 0; i < volumes.size(); i++) {
                Volume vol = volumes.get(i);
                if (AbstractModel.VOLUME_NAME.equals(vol.getName()) && vol.getEmptyDir() != null) {
                    desired.getSpec().getTemplate().getSpec().getVolumes().add(0, volumes.get(i));
                    break;
                }
            }
        } else {
            // We are on persistent storage and changing to ephemeral
            List<Volume> volumes = desired.getSpec().getTemplate().getSpec().getVolumes();
            for (int i = 0; i < volumes.size(); i++) {
                Volume vol = volumes.get(i);
                if (AbstractModel.VOLUME_NAME.equals(vol.getName()) && vol.getEmptyDir() != null) {
                    volumes.remove(i);
                    break;
                }
            }
        }

        return new StatefulSetDiff(current, desired);
    }

    protected Future<String> getUid(String namespace, String podName) {
        Future<String> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                String uid = getPodUid(podOperations.get(namespace, podName));
                future.complete(uid);
            }, true, result.completer()
        );
        return result;
    }

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }
}
