/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Operations for {@code StatefulSets}s, which supports {@link #rollingUpdate(String, String)}
 * in addition to the usual operations.
 */
public class StatefulSetOperations<P> extends AbstractScalableOperations<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>, P> {

    private static final Logger log = LoggerFactory.getLogger(StatefulSetOperations.class.getName());
    private final PodOperations podOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public StatefulSetOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "StatefulSet");
        this.podOperations = new PodOperations(vertx, client);
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
        return rollingUpdate(namespace, name, (podName) -> podOperations.isReady(namespace, podName));
    }

    /**
     * Asynchronously perform a rolling update of all the pods in the StatefulSet identified by the given
     * {@code namespace} and {@code name}, returning a Future that will complete when the rolling update
     * is complete. Starting with pod 0, each pod will be deleted and re-created automatically by the ReplicaSet,
     * once the pod has been recreated then given {@code isReady} function will be polled until it returns true,
     * before the process proceeds with the pod with the next higher number.
     */
    public Future<Void> rollingUpdate(String namespace, String name, Predicate<String> isReady) {
        Future<Void> rollingUpdateFuture = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    final int replicas = get(namespace, name).getSpec().getReplicas();
                    for (int i = 0; i < replicas; i++) {
                        String podName = name + "-" + i;
                        log.info("Rolling pod {}", podName);
                        Future deleted = Future.future();
                        Watcher<Pod> watcher = new RollingUpdateWatcher(deleted);

                        try (Watch ignored = podOperations.watch(namespace, podName, watcher)) {
                            // Delete the pod
                            Future podReconcileFuture = podOperations.reconcile(namespace, podName, null);

                            // TODO do this async
                            while (!podReconcileFuture.isComplete() || !deleted.isComplete()) {
                                log.info("Waiting for pod {} to be deleted", podName);
                                Thread.sleep(1000);
                            }
                            // TODO Check success of podReconcileFuture and deleted futures
                        }

                        while (!isReady.test(podName)) {
                            log.info("Waiting for pod {} to get ready", podName);
                            Thread.sleep(1000);
                        }
                        log.info("Pod {} rolling update complete", podName);
                    }
                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            },
            false,
            rollingUpdateFuture.completer());
        return rollingUpdateFuture;
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
        //private static final Logger log = LoggerFactory.getLogger(RollingUpdateWatcher.class.getName());
        private final Future deleted;

        public RollingUpdateWatcher(Future deleted) {
            this.deleted = deleted;
        }

        @Override
        public void eventReceived(Action action, Pod pod) {
            switch (action) {
                case DELETED:
                    log.info("Pod has been deleted");
                    deleted.complete();
                    break;
                case ADDED:
                case MODIFIED:
                    log.info("Ignored action {} while waiting for Pod deletion", action);
                    break;
                case ERROR:
                    log.error("Error while waiting for Pod deletion");
                    break;
                default:
                    log.error("Unknown action {} while waiting for pod deletion", action);
            }
        }

        @Override
        public void onClose(KubernetesClientException e) {
            if (e != null && !deleted.isComplete()) {
                log.error("Kubernetes watcher has been closed with exception!", e);
                deleted.fail(e);
            } else {
                log.info("Kubernetes watcher has been closed!");
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


        long operationTimeoutMs = 60_000L;

        // ... then wait for the SS to be ready...
        crt.compose(res -> readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs).map(res))
        // ... then wait for all the pods to be ready
        .compose(res -> podReadiness(namespace, desired, 1_000, operationTimeoutMs).map(res))
        .compose(res -> {
            result.complete(res);
        }, result);
        // TODO I need to block until things are ready
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
        log.info("Patching {} resource {} in namespace {} with {}", resourceKind, name, namespace, desired);
        operation().inNamespace(namespace).withName(name).cascading(false).patch(desired);
        log.info("{} {} in namespace {} has been patched", resourceKind, name, namespace);
        return Future.succeededFuture(ReconcileResult.patched(null));
    }
}
