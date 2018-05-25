/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Operations for {@code StatefulSets}s, which supports {@link #rollingUpdate(String, String)}
 * in addition to the usual operations.
 */
public class StatefulSetOperator<P> extends AbstractScalableResourceOperator<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>, P> {

    private static final Logger log = LogManager.getLogger(StatefulSetOperator.class.getName());
    private final PodOperator podOperations;
    private final long operationTimeoutMs;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        super(vertx, client, "StatefulSet");
        this.podOperations = new PodOperator(vertx, client);
        this.operationTimeoutMs = operationTimeoutMs;
    }

    @Override
    protected MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> operation() {
        return client.apps().statefulSets();
    }

    /**
     * Asynchronously perform a rolling update of all the pods in the StatefulSet identified by the given
     * {@code namespace} and {@code name}, returning a Future that will complete when the rolling update
     * is complete. Starting with pod 0, each pod will be deleted and re-created automatically by the ReplicaSet,
     * once the pod has been recreated and is ready the process proceeds with the pod with the next higher number.
     */
    public Future<Void> rollingUpdate(String namespace, String name) {
        return rollingUpdate(namespace, name,
            podName -> podOperations.isReady(namespace, podName));
    }

    private Future<Integer> getReplicas(String namespace, String name) {
        Future<Integer> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    Integer replicas = get(namespace, name).getSpec().getReplicas();
                    future.complete(replicas);
                } catch (Throwable t) {
                    future.fail(t);
                }
            }, true,
            result.completer());
        return result;
    }

    /**
     * Asynchronously perform a rolling update of all the pods in the StatefulSet identified by the given
     * {@code namespace} and {@code name}, returning a Future that will complete when the rolling update
     * is complete. Starting with pod 0, each pod will be deleted and re-created automatically by the ReplicaSet,
     * once the pod has been recreated then given {@code isReady} function will be polled until it returns true,
     * before the process proceeds with the pod with the next higher number.
     */
    public Future<Void> rollingUpdate(String namespace, String name, Predicate<String> isReady) {
        log.info("Starting rolling update of {}/{}", namespace, name);
        Future<Void> rollingUpdateFuture = Future.future();
        // Get the number of replicas
        getReplicas(namespace, name).compose(replicas -> {
            Future<Void> f = Future.succeededFuture();
            // Then for each replicas, restart it
            for (int i = 0; i < replicas; i++) {
                String podName = name + "-" + i;
                f = f.compose(ignored -> restartPod(namespace, name, isReady, podName));
            }
            return f;
        }).setHandler(rollingResult -> {
            rollingUpdateFuture.handle(rollingResult);
        });
        return rollingUpdateFuture;
    }

    private <T> Future<Boolean> p(Predicate<T> isReady, T argument) {
        Future<Boolean> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    future.complete(isReady.test(argument));
                } catch (Throwable t) {
                    future.fail(t);
                }
            }, true,
            result.completer());
        return result;
    }

    private Future<Void> restartPod(String namespace, String name, Predicate<String> isReady, String podName) {
        Future<Void> result = Future.future();
        log.info("Rolling update of {}/{}: Rolling pod {}", namespace, name, podName);
        Future<Void> deleted = Future.future();
        Future<CompositeFuture> deleteFinished = Future.future();
        Watcher<Pod> watcher = new RollingUpdateWatcher(deleted);
        Watch watch = podOperations.watch(namespace, podName, watcher);
        // Delete the pod
        log.debug("Rolling update of {}/{}: Waiting for pod {} to be deleted", namespace, name, podName);
        Future podReconcileFuture = podOperations.reconcile(namespace, podName, null);
        CompositeFuture.join(podReconcileFuture, deleted).setHandler(deleteResult -> {
            watch.close();
            if (deleteResult.succeeded()) {
                log.debug("Rolling update of {}/{}: Pod {} was deleted", namespace, name, podName);
            }
            deleteFinished.handle(deleteResult);
        });
        deleteFinished.compose(ix -> {
            log.debug("Rolling update of {}/{}: Waiting for new pod {} to get ready", namespace, name, podName);
            Future<Void> readyFuture = Future.future();
            vertx.setPeriodic(1_000, timerId -> {
                p(isReady, podName).setHandler(x -> {
                    if (x.succeeded()) {
                        if (x.result()) {
                            vertx.cancelTimer(timerId);
                            readyFuture.complete();
                        }
                        // else not ready
                    } else {
                        vertx.cancelTimer(timerId);
                        readyFuture.fail(x.cause());
                    }
                });
            });
            return readyFuture;
        }).setHandler(result);
        return result;
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

    static class RollingUpdateWatcher implements Watcher<Pod> {
        //private static final Logger log = LogManager.getLogger(RollingUpdateWatcher.class.getName());
        private final Future deleted;

        public RollingUpdateWatcher(Future deleted) {
            this.deleted = deleted;
        }

        @Override
        public void eventReceived(Action action, Pod pod) {
            String podName = pod.getMetadata().getName();
            switch (action) {
                case DELETED:
                    log.debug("Pod {} has been deleted", podName);
                    deleted.complete();
                    break;
                case ADDED:
                case MODIFIED:
                    log.debug("Ignored action {} on pod {} while waiting for Pod deletion", action, podName);
                    break;
                case ERROR:
                    log.error("Error while waiting for Pod deletion");
                    break;
                default:
                    log.error("Unknown action {} on pod {} while waiting for pod deletion", action, podName);
            }
        }

        @Override
        public void onClose(KubernetesClientException e) {
            if (e != null && !deleted.isComplete()) {
                log.error("Rolling update watcher has been closed with exception", e);
                deleted.fail(e);
            } else {
                log.debug("Rolling update watcher has been closed");
            }
        }
    }

    public String getPodName(StatefulSet desired, int podId) {
        return desired.getMetadata().getName() + "-" + podId;
    }

    @Override
    protected Future<ReconcileResult<P>> internalCreate(String namespace, String name, StatefulSet desired) {
        // Create the SS...
        Future<ReconcileResult<P>> result = Future.future();
        Future<ReconcileResult<P>> crt = super.internalCreate(namespace, name, desired);

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
    protected Future<ReconcileResult<P>> internalPatch(String namespace, String name, StatefulSet current, StatefulSet desired) {
        // Don't scale via patch
        desired.getSpec().setReplicas(current.getSpec().getReplicas());
        if (log.isTraceEnabled()) {
            log.trace("Patching {} {}/{} to match desired state {}", resourceKind, namespace, name, desired);
        } else {
            log.debug("Patching {} {}/{}", resourceKind, namespace, name);
        }
        operation().inNamespace(namespace).withName(name).cascading(false).patch(desired);
        log.debug("Patched {} {}/{}", resourceKind, namespace, name);
        return Future.succeededFuture(ReconcileResult.patched(null));
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
}
