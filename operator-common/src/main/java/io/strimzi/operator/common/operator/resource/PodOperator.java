/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.util.concurrent.CompletionStage;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.StrimziFuture;

/**
 * Operations for {@code Pod}s, which support {@link #isReady(String, String)}.
 */
public class PodOperator extends AbstractReadyNamespacedResourceOperator<KubernetesClient, Pod, PodList, PodResource> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(PodOperator.class);
    private static final String NO_UID = "NULL";

    /**
     * Constructor
     * @param client The Kubernetes client
     */
    public PodOperator(KubernetesClient client) {
        super(client, "Pods");
    }

    @Override
    protected MixedOperation<Pod, PodList, PodResource> operation() {
        return client.pods();
    }

    /**
     * Asynchronously delete the given pod, return a CompletionStage which completes when the Pod has been recreated.
     * Note: The pod might not be "ready" when the returned Future completes.
     * @param reconciliation The reconciliation
     * @param pod The pod to be restarted
     * @param timeoutMs Timeout of the deletion
     * @return a stage which completes when the Pod has been recreated
     */
    public StrimziFuture<Void> restart(Reconciliation reconciliation, Pod pod, long timeoutMs) {
        long pollingIntervalMs = 1_000;
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        StrimziFuture<Void> deleteFinished = new StrimziFuture<>();
        LOGGER.infoCr(reconciliation, "Rolling pod {}", podName);

        // Determine generation of deleted pod
        String deleted = getPodUid(pod);

        // Delete the pod
        LOGGER.debugCr(reconciliation, "Waiting for pod {} to be deleted", podName);
        CompletionStage<Void> podReconcileFuture =
                reconcile(reconciliation, namespace, podName, null)
                        .thenCompose(ignore -> waitFor(reconciliation, namespace, podName, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                            // predicate - changed generation means pod has been updated
                            String newUid = getPodUid(get(namespace, podName));
                            boolean done = !deleted.equals(newUid);

                            if (done) {
                                LOGGER.debugCr(reconciliation, "Rolling pod {} finished", podName);
                            }

                            return done;
                        }));

        podReconcileFuture.whenComplete((deleteResult, thrown) -> {
            if (thrown == null) {
                LOGGER.debugCr(reconciliation, "Pod {} was deleted", podName);
                deleteFinished.complete(null);
            } else {
                deleteFinished.completeExceptionally(thrown);
            }
        });
        return deleteFinished;
    }

    private static String getPodUid(HasMetadata resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }
}
