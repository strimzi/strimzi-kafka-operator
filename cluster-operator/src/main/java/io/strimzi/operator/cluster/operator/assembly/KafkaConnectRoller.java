/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.PodRevision;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

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
     * @param podNamesToConsider    Names of the Pods that should be considered for rolling
     * @param podNeedsRestart       Function that evaluates the PodSet and Pods and decides if restart of the Pod is
     *                              needed or not
     *
     * @return  Future which completes when the rolling update is done
     */
    public Future<Void> maybeRoll(List<String> podNamesToConsider, Function<Pod, RestartReasons> podNeedsRestart)    {
        return podOperator.listAsync(reconciliation.namespace(), connect.getSelectorLabels())
                .compose(pods -> Future.succeededFuture(prepareRollingOrder(podNamesToConsider, pods)))
                .compose(rollingOrder -> maybeRollPods(podNeedsRestart, rollingOrder));
    }

    /* test */ Queue<String> prepareRollingOrder(List<String> podNamesToConsider, List<Pod> pods)   {
        Deque<String> rollingOrder = new ArrayDeque<>();

        for (String podName : podNamesToConsider)   {
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
     * @param podNeedsRestart   Function that evaluates the PodSet and Pods and decides if restart of the Pod is needed
     *                          or not
     * @param rollingOrder      Queue with the pod names in the order of their rolling
     *
     * @return  Future which completes when all pods were rolled / considered for rolling
     */
    private Future<Void> maybeRollPods(Function<Pod, RestartReasons> podNeedsRestart,
                                       Queue<String> rollingOrder)  {
        String podName = rollingOrder.poll();

        if (podName != null)    {
            // The queue is not empty. We consider rolling of this pod and call this method again to handle the next pod
            return maybeRollPod(podNeedsRestart, podName)
                    .compose(i -> maybeRollPods(podNeedsRestart, rollingOrder));
        } else {
            // Queue is empty => we return completely
            return Future.succeededFuture();
        }
    }

    /**
     * Checks given pod if it needs rolling and rolls it if needed. It checks the pod readiness afterward and waits
     * for it if needed. The Pod is rolled based on the callback that decides if rolling is needed based on the Pod
     * definition passed as a parameter.
     *
     * @param podNeedsRestart   Function that evaluates the PodSet and Pods and decides if restart of the Pod is needed
     *                          or not
     * @param podName           Name of the pod which should be considered
     *
     * @return  Future which completes when the pod is maybe rolled and ready
     */
    /* test */ Future<Void> maybeRollPod(Function<Pod, RestartReasons> podNeedsRestart,
                                         String podName) {
        return podOperator.getAsync(reconciliation.namespace(), podName)
                .compose(pod -> {
                    if (pod == null) {
                        LOGGER.debugCr(reconciliation, "Pod {} does not exist => waiting for its creation", podName);
                        return Future.succeededFuture();
                    } else {
                        RestartReasons restartReasons = podNeedsRestart.apply(pod);

                        if (restartReasons.shouldRestart())  {
                            // Pods changed and needs rolling
                            LOGGER.infoCr(reconciliation, "Rolling pod {}: {}", podName, restartReasons.getAllReasonNotes());
                            return podOperator.deleteAsync(reconciliation, reconciliation.namespace(), podName, false);
                        } else {
                            // Pod exists and does not need to be rolled
                            LOGGER.debugCr(reconciliation, "Pod {} does not need to be rolled", podName);
                            return Future.succeededFuture();
                        }
                    }
                })
                .compose(i -> {
                    LOGGER.debugCr(reconciliation, "Waiting for pod {} to become ready", podName);
                    return podOperator.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, operationTimeoutMs);
                });
    }

    /**
     * Checks if the Pod needs a rolling restart. This method is used in regular rolling updates and checks if the Pod
     * matches the PodSet or not.
     *
     * @param podSet    PodSet with the desired Pod definition
     * @param pod       The current definition of the Pod
     *
     * @return  RestartReasons object indicating whether a restart is needed and why.
     */
    public static RestartReasons needsRollingRestart(StrimziPodSet podSet, Pod pod) {
        if (PodRevision.hasChanged(pod, podSet)) {
            return RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION);
        } else {
            return RestartReasons.empty();
        }
    }
}