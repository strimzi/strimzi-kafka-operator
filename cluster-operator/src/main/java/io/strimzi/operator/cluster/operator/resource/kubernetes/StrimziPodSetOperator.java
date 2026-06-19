/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetList;
import io.strimzi.operator.common.Reconciliation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Operator for {@code StrimziPodSet}s
 */
public class StrimziPodSetOperator extends CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> {

    /**
     * Constructs the StrimziPodSet operator
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client.
     */
    public StrimziPodSetOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, StrimziPodSet.class, StrimziPodSetList.class, StrimziPodSet.RESOURCE_KIND);
    }

    /**
     * StrimziPodSetOperator overrides this method in order to use replace instead of patch.
     *
     * @param name          Name of the resource
     * @param desired       Desired resource
     *
     * @return  The patched or replaced resource
     */
    @Override
    protected CompletionStage<StrimziPodSet> patchOrReplace(String namespace, String name, StrimziPodSet desired)   {
        return CompletableFuture.supplyAsync(() -> operation().inNamespace(namespace).resource(desired).update(), asyncExecutor);
    }

    /**
     * Waits for StrimziPodSet to get ready
     *
     * @param reconciliation    Reconciliation marker
     * @param namespace         Namespace of the StrimziPodSet
     * @param name              Name of the StrimziPodSet
     * @param pollIntervalMs    How often should it poll for readiness
     * @param timeoutMs         How long should it wait for the resource to get ready
     *
     * @return  A future which completes when the resource is ready or times out
     */
    public CompletionStage<Void> readiness(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
        return waitFor(reconciliation, namespace, name, pollIntervalMs, timeoutMs, this::isReady);
    }

    /**
     * Check if the PodSet is in the Ready state. The PodSet is ready when all the following conditions are fulfilled:
     *     - All pods are created
     *     - All pods are up-to-date
     *     - All pods are ready
     *
     * @param namespace The namespace
     * @param name      The name of the StrimziPodSet
     *
     * @return  True when the StrimziPodSet is ready. False otherwise.
     */
    public boolean isReady(String namespace, String name) {
        StrimziPodSet podSet = operation().inNamespace(namespace).withName(name).get();

        if (podSet != null) {
            int replicas = podSet.getSpec().getPods().size();

            return podSet.getStatus() != null
                    && replicas == podSet.getStatus().getPods()
                    && replicas == podSet.getStatus().getReadyPods();
        } else {
            return false;
        }
    }
}
