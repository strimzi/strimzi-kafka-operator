/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * ZooKeeperRoller helps to roll ZooKeeper cluster. It uses the ZooKeeperLeaderFinder to find the leader which is
 * rolled first.
 */
public class ZooKeeperRoller {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZooKeeperRoller.class.getName());

    private final PodOperator podOperator;
    private final ZookeeperLeaderFinder leaderFinder;
    private final long operationTimeoutMs;

    public ZooKeeperRoller(PodOperator podOperator, ZookeeperLeaderFinder leaderFinder, long operationTimeoutMs) {
        this.podOperator = podOperator;
        this.leaderFinder = leaderFinder;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    /**
     * Asynchronously perform a rolling update of all the pods belonging to the ZooKeeper cluster and returns a future
     * which completes when all required pods are rolled and ready again. It uses the ZooKeeperLeaderFinder to find the
     * leader node and roll it last.
     *
     * @param reconciliation    The reconciliation
     * @param selectorLabels    The selector labels to find the pods
     * @param podRestart        Function that returns a list is reasons why the given pod needs to be restarted, or an empty list if the pod does not need to be restarted.
     * @param clusterCaSecret   Secret with cluster CA certificates
     * @param coKeySecret       Secret with the Cluster operator certificates
     *
     * @return A future that completes when any necessary rolling has been completed.
     */
    public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, Labels selectorLabels, Function<Pod, List<String>> podRestart, Secret clusterCaSecret, Secret coKeySecret) {
        String namespace = reconciliation.namespace();

        return podOperator.listAsync(namespace, selectorLabels)
                .compose(pods -> {
                    Map<String, List<String>> podsToRoll = new HashMap<>(pods.size());
                    boolean needsRolling = false;

                    for (Pod pod : pods)    {
                        List<String> restartReasons = podRestart.apply(pod);
                        String podName = pod.getMetadata().getName();

                        if (restartReasons != null && !restartReasons.isEmpty())    {
                            LOGGER.debugCr(reconciliation, "Pod {} should be rolled due to {}", podName, restartReasons);
                            podsToRoll.put(podName, restartReasons);
                            needsRolling = true;
                        } else {
                            LOGGER.debugCr(reconciliation, "Pod {} does not need to be rolled", podName);
                            podsToRoll.put(podName, null);
                        }
                    }

                    if (needsRolling)   {
                        return Future.succeededFuture(podsToRoll);
                    } else {
                        return Future.succeededFuture(null);
                    }
                }).compose(podsToRoll -> {
                    if (podsToRoll != null)  {
                        Promise<Void> promise = Promise.promise();
                        Future<String> leaderFuture = leaderFinder.findZookeeperLeader(reconciliation, podsToRoll.keySet(), clusterCaSecret, coKeySecret);

                        leaderFuture.compose(leader -> {
                            LOGGER.debugCr(reconciliation, "Zookeeper leader is " + (ZookeeperLeaderFinder.UNKNOWN_LEADER.equals(leader) ? "unknown" : "pod " + leader));
                            Future<Void> fut = Future.succeededFuture();

                            // Then roll each non-leader pod => the leader is rolled last
                            for (Map.Entry<String, List<String>> podEntry : podsToRoll.entrySet())  {
                                if (podEntry.getValue() != null && !podEntry.getValue().isEmpty()) {
                                    if (!podEntry.getKey().equals(leader)) {
                                        LOGGER.debugCr(reconciliation, "Restarting non-leader pod {}", podEntry.getKey());
                                        // roll the pod and wait until it is ready
                                        // this prevents rolling into faulty state (note: this applies just for ZK pods)
                                        fut = fut.compose(ignore -> restartPod(reconciliation, podEntry.getKey(), podEntry.getValue()));
                                    } else {
                                        LOGGER.debugCr(reconciliation, "Deferring restart of leader {}", podEntry.getKey());
                                    }
                                }
                            }

                            // Check if we have a leader and if it needs tolling
                            if (ZookeeperLeaderFinder.UNKNOWN_LEADER.equals(leader) || podsToRoll.get(leader) == null || podsToRoll.get(leader).isEmpty()) {
                                return fut;
                            } else {
                                // Roll the leader pod
                                return fut.compose(ar -> {
                                    // the leader is rolled as the last
                                    LOGGER.debugCr(reconciliation, "Restarting leader pod (previously deferred) {}", leader);
                                    return restartPod(reconciliation, leader, podsToRoll.get(leader));
                                });
                            }
                        }).onComplete(promise);

                        return promise.future();
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Restarts the Pod => deletes it, waits until it is really deleted and until the new pod starts and gets ready.
     *
     * @param reconciliation Reconciliation object
     * @param podName The name of the Pod to possibly restart.
     * @param reasons Reasons for the restart
     *
     * @return a Future which completes when the given (possibly recreated) pod is ready.
     */
    Future<Void> restartPod(Reconciliation reconciliation, String podName, List<String> reasons) {
        long pollingIntervalMs = 1_000;

        LOGGER.infoCr(reconciliation, "Rolling pod {} due to {}", podName, reasons);
        return podOperator.getAsync(reconciliation.namespace(), podName)
                .compose(pod -> podOperator.restart(reconciliation, pod, operationTimeoutMs))
                .compose(ignore -> {
                    LOGGER.debugCr(reconciliation, "Waiting for readiness of pod {}", podName);
                    return podOperator.readiness(reconciliation, reconciliation.namespace(), podName, pollingIntervalMs, operationTimeoutMs);
                });
    }
}