/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Pod}s, which support {@link #isReady(String, String)} and
 * {@link #watch(String, String, Watcher)} in addition to the usual operations.
 */
public class PodOperator extends AbstractReadyResourceOperator<KubernetesClient, Pod, PodList, PodResource<Pod>> {

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
    protected MixedOperation<Pod, PodList, PodResource<Pod>> operation() {
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
     * @param reconciliation The reconciliation
     * @param pod The pod to be restarted
     * @param timeoutMs Timeout of the deletion
     * @return a Future which completes when the Pod has been recreated
     */
    public Future<Void> restart(Reconciliation reconciliation, Pod pod, long timeoutMs) {
        long pollingIntervalMs = 1_000;
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        Promise<Void> deleteFinished = Promise.promise();
        reconciliationLogger.infoCr(reconciliation, "Rolling pod {}", podName);

        // Determine generation of deleted pod
        String deleted = getPodUid(pod);

        // Delete the pod
        reconciliationLogger.debugCr(reconciliation, "Waiting for pod {} to be deleted", podName);
        Future<Void> podReconcileFuture =
                reconcile(reconciliation, namespace, podName, null).compose(ignore -> {
                    Future<Void> del = waitFor(reconciliation, namespace, podName, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                        // predicate - changed generation means pod has been updated
                        String newUid = getPodUid(get(namespace, podName));
                        boolean done = !deleted.equals(newUid);
                        if (done) {
                            reconciliationLogger.debugCr(reconciliation, "Rolling pod {} finished", podName);
                        }
                        return done;
                    });
                    return del;
                });

        podReconcileFuture.onComplete(deleteResult -> {
            if (deleteResult.succeeded()) {
                reconciliationLogger.debugCr(reconciliation, "Pod {} was deleted", podName);
            }
            deleteFinished.handle(deleteResult);
        });
        return deleteFinished.future();
    }

    private static String getPodUid(HasMetadata resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }
}
