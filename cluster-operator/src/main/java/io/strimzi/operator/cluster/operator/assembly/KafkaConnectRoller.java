/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Queue;

/**
 * This class contains the methods for rolling Kafka Connect and Kafka Mirror Maker 2 clusters based on StrimziPodSets.
 */
public class KafkaConnectRoller {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectRoller.class.getName());

    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final KafkaConnectCluster connect;

    private final PodOperator podOperator;

    /**
     * Constructs the Manual Pod Cleaner.
     *
     * @param reconciliation        Reconciliation marker
     * @param connect               Kafka Connect Cluster model
     * @param operationTimeoutMs    Operations timeout in milliseconds
     * @param podOperator           Resource Operator for managing Pods
     */
    public KafkaConnectRoller(
            Reconciliation reconciliation,
            KafkaConnectCluster connect,
            long operationTimeoutMs,
            PodOperator podOperator
    ) {
        this.reconciliation = reconciliation;
        this.connect = connect;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperator = podOperator;
    }

    /**
     * Goes through the Kafka Connect pods, considers them for rolling, and checks their readiness. If there are any
     * pods which do not exist or are not ready, they will be handled first to avoid killing the whole cluster in
     * subsequent reconciliations.
     *
     * @param podSet    The current StrimziPodSet resource which the Pods are compared against when checking if they
     *                  are up-to-date
     *
     * @return  Future which completes when the rolling update is done
     */
    public Future<Void> maybeRoll(StrimziPodSet podSet)    {
        return podOperator.listAsync(reconciliation.namespace(), connect.getSelectorLabels())
                .compose(pods -> Future.succeededFuture(prepareRollingOrder(pods)))
                .compose(rollingOrder -> maybeRollPods(podSet, rollingOrder));
    }

    /* test */ Queue<String> prepareRollingOrder(List<Pod> pods)   {
        Deque<String> rollingOrder = new ArrayDeque<>();

        for (int i = 0; i < connect.getReplicas(); i++) {
            String podName = connect.getPodName(i);
            Pod matchingPod = pods.stream().filter(pod -> podName.equals(pod.getMetadata().getName())).findFirst().orElse(null);

            if (matchingPod == null || !Readiness.isPodReady(matchingPod))    {
                // Non-existing or unready pods are handled first
                // This helps to avoid rolling all pods into some situation where they would be all failing
                rollingOrder.addFirst(podName);
            } else {
                // Ready pods are rolled only at the end
                rollingOrder.addLast(podName);
            }
        }

        return rollingOrder;
    }

    /**
     * Goes through the pods in given order and considers them for rolling. It checks if the first pod in the queue
     * needs rolling and checks its readiness. And then calls itself to move to the next pod.
     *
     * @param podSet        The current StrimziPodSet resource which the Pods are compared against when checking if they
     *                      are up-to-date
     * @param rollingOrder  Queue with the pod names in the order of their rolling
     *
     * @return  Future which completes when all pods were rolled / considered for rolling
     */
    private Future<Void> maybeRollPods(StrimziPodSet podSet,
                                       Queue<String> rollingOrder)  {
        String podName = rollingOrder.poll();

        if (podName != null)    {
            // The queue is not empty. We consider rolling of this pod and call this method again to handle the next pod
            return maybeRollPod(podSet, podName)
                    .compose(i -> maybeRollPods(podSet, rollingOrder));
        } else {
            // Queue is empty => we return completely
            return Future.succeededFuture();
        }
    }

    /**
     * Checks given pod if it needs rolling and rolls it if needed. It checks the pod readiness afterwards and waits
     * for it if needed. The Pod is rolled when its revision doesn't match the desired revision from the StrimziPodSet.
     *
     * @param podSet    The current StrimziPodSet resource which the Pods are compared against when checking if they
     *                  are up-to-date
     * @param podName   Name of the pod which should be considered
     *
     * @return  Future which completes when the pod is maybe rolled and ready
     */
    /* test */ Future<Void> maybeRollPod(StrimziPodSet podSet,
                                      String podName) {
        return podOperator.getAsync(reconciliation.namespace(), podName)
                .compose(pod -> {
                    if (pod == null)    {
                        LOGGER.debugCr(reconciliation, "Pod {} does not exist => waiting for its creation", podName);
                        return Future.succeededFuture();
                    } else if (PodRevision.hasChanged(pod, podSet)) {
                        // Pods changed and needs rolling
                        LOGGER.infoCr(reconciliation, "Rolling pod {} (Pod revision changed)", podName);
                        return podOperator.deleteAsync(reconciliation, reconciliation.namespace(), podName, false);
                    } else {
                        // Pod exists and does not need to be rolled
                        LOGGER.debugCr(reconciliation, "Pod {} does not need to be rolled", podName);
                        return Future.succeededFuture();
                    }
                })
                .compose(i -> {
                    LOGGER.debugCr(reconciliation, "Waiting for pod {} to become ready", podName);
                    return podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, operationTimeoutMs);
                });
    }
}