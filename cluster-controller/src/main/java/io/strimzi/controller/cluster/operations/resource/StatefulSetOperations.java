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

/**
 * Operations for {@code StatefulSets}s, which supports {@link #rollingUpdate(String, String)}
 * in addition to the usual operations.
 */
public class StatefulSetOperations extends AbstractScalableOperations<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> {

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

    private void rollingUpdate(String namespace, String name) throws Exception {
        final int replicas = get(namespace, name).getSpec().getReplicas();
        for (int i = 0; i < replicas; i++) {
            String podName = name + "-" + i;
            log.info("Rolling pod {}", podName);
            Future deleted = Future.future();
            Watcher<Pod> watcher = new RollingUpdateWatcher(deleted);

            Watch watch = podOperations.watch(namespace, podName, watcher);
            Future fut = podOperations.reconcile(namespace, podName, null);

            // TODO do this async
            while (!fut.isComplete() || !deleted.isComplete()) {
                log.info("Waiting for pod {} to be deleted", podName);
                Thread.sleep(1000);
            }
            // TODO Check success of fut and deleted futures

            watch.close();

            while (!podOperations.isReady(namespace, podName)) {
                log.info("Waiting for pod {} to get ready", podName);
                Thread.sleep(1000);
            }

            log.info("Pod {} rolling update complete", podName);
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
    protected void internalCreate(String namespace, String name, StatefulSet desired, Future<Void> future) {
        final int replicas = desired.getSpec().getReplicas();

        // Create the SS...
        Future crt = Future.future();
        super.internalCreate(namespace, name, desired, crt);

        long operationTimeoutMs = 60_000L;

        crt
        // ... then wait for the SS to be ready...
        .compose(res -> readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs))
        // ... then wait for all the pods to be ready
        .compose(res -> podReadiness(namespace, desired, 1_000, operationTimeoutMs))
        .compose(res -> {
            future.complete();
        }, future);
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
    protected void internalPatch(String namespace, String name, StatefulSet desired, Future<Void> future) {
        try {
            log.info("Patching {} resource {} in namespace {} with {}", resourceKind, name, namespace, desired);
            operation().inNamespace(namespace).withName(name).cascading(false).patch(desired);
            log.info("{} {} in namespace {} has been patched", resourceKind, name, namespace);
            rollingUpdate(namespace, desired.getMetadata().getName());
            future.complete();
        } catch (Exception e) {
            log.error("Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            future.fail(e);
        }
    }
}
