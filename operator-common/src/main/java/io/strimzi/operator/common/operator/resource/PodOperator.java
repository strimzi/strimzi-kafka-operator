/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Pod}s, which support {@link #isReady(String, String)} and
 * {@link #watch(String, String, Watcher)} in addition to the usual operations.
 */
public class PodOperator extends AbstractReadyResourceOperator<KubernetesClient, Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> {

    private static final String NO_UID = "NULL";

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public PodOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Pods");
    }

    @Override
    protected MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation() {
        return client.pods();
    }

    /**
     * Watch the pod identified by the given {@code namespace} and {@code name} using the given {@code watcher}.
     * @param namespace The namespace
     * @param name The name
     * @param watcher The watcher
     * @return The watch
     */
    public Watch watch(String namespace, String name, Watcher<Pod> watcher) {
        return operation().inNamespace(namespace).withName(name).watch(watcher);
    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be "ready" when the returned Future completes.
     * @param logContext Some context (for logging)
     * @param pod The pod to be restarted
     * @param timeoutMs Timeout of the deletion
     * @return a Future which completes when the Pod has been recreated
     */
    public Future<Void> restart(String logContext, Pod pod, long timeoutMs) {
        long pollingIntervalMs = 1_000;
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        Future<Void> deleteFinished = Future.future();
        log.info("{}: Rolling pod {}", logContext, podName);

        // Determine generation of deleted pod
        String deleted = getPodUid(pod);

        // Delete the pod
        log.debug("{}}: Waiting for pod {} to be deleted", logContext, podName);
        Future<Void> podReconcileFuture =
                reconcile(namespace, podName, null).compose(ignore -> {
                    Future<Void> del = waitFor(namespace, podName, pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                        // predicate - changed generation means pod has been updated
                        String newUid = getPodUid(get(namespace, podName));
                        boolean done = !deleted.equals(newUid);
                        if (done) {
                            log.debug("Rolling pod {} finished", podName);
                        }
                        return done;
                    });
                    return del;
                });

        podReconcileFuture.setHandler(deleteResult -> {
            if (deleteResult.succeeded()) {
                log.debug("{}: Pod {} was deleted", logContext, podName);
            }
            deleteFinished.handle(deleteResult);
        });
        return deleteFinished;
    }

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }
}
