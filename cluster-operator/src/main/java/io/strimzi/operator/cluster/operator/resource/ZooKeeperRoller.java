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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ZooKeeperRoller helps to roll ZooKeeper cluster. It uses the ZooKeeperLeaderFinder to find the leader which is
 * rolled last.
 */
public class ZooKeeperRoller {

    private static final long READINESS_POLLING_INTERVAL_MS = 1_000;

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZooKeeperRoller.class.getName());

    private final PodOperator podOperator;
    private final ZookeeperLeaderFinder leaderFinder;
    private final long operationTimeoutMs;

    /**
     * Constructor
     *
     * @param podOperator           Pod operator
     * @param leaderFinder          ZooKeeper Leader Finder
     * @param operationTimeoutMs    Operation timeout in milliseconds
     */
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
                    ZookeeperClusterRollContext clusterRollContext = new ZookeeperClusterRollContext();

                    for (Pod pod : pods)    {
                        List<String> restartReasons = podRestart.apply(pod);
                        final boolean ready = podOperator.isReady(namespace, pod.getMetadata().getName());
                        ZookeeperPodContext podContext = new ZookeeperPodContext(pod, restartReasons, ready);
                        if (restartReasons != null && !restartReasons.isEmpty())    {
                            LOGGER.debugCr(reconciliation, "Pod {} should be rolled due to {}", podContext.getPodName(), restartReasons);
                        } else {
                            LOGGER.debugCr(reconciliation, "Pod {} does not need to be rolled", podContext.getPodName());
                        }
                        clusterRollContext.add(podContext);
                    }

                    if (clusterRollContext.requiresRestart())   {
                        return Future.succeededFuture(clusterRollContext);
                    } else {
                        return Future.succeededFuture(null);
                    }
                }).compose(clusterRollContext -> {
                    if (clusterRollContext != null)  {
                        Promise<Void> promise = Promise.promise();
                        Future<String> leaderFuture = leaderFinder.findZookeeperLeader(reconciliation, clusterRollContext.podNames(), clusterCaSecret, coKeySecret);

                        leaderFuture.compose(leader -> {
                            LOGGER.debugCr(reconciliation, "Zookeeper leader is " + (ZookeeperLeaderFinder.UNKNOWN_LEADER.equals(leader) ? "unknown" : "pod " + leader));
                            Future<Void> fut = Future.succeededFuture();

                            // Then roll each non-leader pod => the leader is rolled last
                            for (ZookeeperPodContext podContext : clusterRollContext.getPodContextsWithNonReadyFirst())  {
                                if (podContext.requiresRestart() && !podContext.getPodName().equals(leader)) {
                                    LOGGER.debugCr(reconciliation, "Pod {} needs to be restarted", podContext.getPodName());
                                    // roll the pod and wait until it is ready
                                    // this prevents rolling into faulty state (note: this applies just for ZK pods)
                                    fut = fut.compose(ignore -> restartPod(reconciliation, podContext.getPodName(), podContext.reasonsToRestart));
                                } else {
                                    if (podContext.requiresRestart()) {
                                        LOGGER.debugCr(reconciliation, "Deferring restart of leader {}", podContext.getPodName());
                                    } else {
                                        LOGGER.debugCr(reconciliation, "Pod {} does not need to be restarted", podContext.getPodName());
                                    }
                                    fut = fut.compose(ignore -> podOperator.readiness(reconciliation, reconciliation.namespace(), podContext.getPodName(), READINESS_POLLING_INTERVAL_MS, operationTimeoutMs));
                                }
                            }

                            // Check if we have a leader and if it needs rolling
                            if (ZookeeperLeaderFinder.UNKNOWN_LEADER.equals(leader) || clusterRollContext.get(leader) == null || !clusterRollContext.get(leader).requiresRestart()) {
                                return fut;
                            } else {
                                // Roll the leader pod
                                return fut.compose(ar -> {
                                    // the leader is rolled as the last
                                    LOGGER.debugCr(reconciliation, "Restarting leader pod (previously deferred) {}", leader);
                                    return restartPod(reconciliation, leader, clusterRollContext.get(leader).reasonsToRestart);
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
        LOGGER.infoCr(reconciliation, "Rolling pod {} due to {}", podName, reasons);
        return podOperator.getAsync(reconciliation.namespace(), podName)
                .compose(pod -> podOperator.restart(reconciliation, pod, operationTimeoutMs))
                .compose(ignore -> {
                    LOGGER.debugCr(reconciliation, "Waiting for readiness of pod {}", podName);
                    return podOperator.readiness(reconciliation, reconciliation.namespace(), podName, READINESS_POLLING_INTERVAL_MS, operationTimeoutMs);
                });
    }

    private static class ZookeeperClusterRollContext {

        private final List<ZookeeperPodContext> podContexts = new ArrayList<>();

        private ZookeeperClusterRollContext() {
        }

        public List<ZookeeperPodContext> getPodContextsWithNonReadyFirst() {
            return podContexts.stream().sorted((contextA, contextB) -> Boolean.compare(contextA.ready, contextB.ready)).collect(Collectors.toList());
        }

        public void add(final ZookeeperPodContext podContext) {
            podContexts.add(podContext);
        }

        public boolean requiresRestart() {
            return podContexts.stream().anyMatch(ZookeeperPodContext::requiresRestart);
        }

        public Set<String> podNames() {
            return podContexts.stream().map(ZookeeperPodContext::getPodName).collect(Collectors.toSet());
        }

        public ZookeeperPodContext get(final String podName) {
            return podContexts.stream().filter(podContext -> podContext.getPodName().equals(podName)).findAny().orElse(null);
        }
    }

    private static class ZookeeperPodContext {

        private final Pod pod;

        private final boolean ready;

        private final List<String> reasonsToRestart = new ArrayList<>();

        private ZookeeperPodContext(final Pod pod, final List<String> reasonsToRestart, final boolean ready) {
            this.pod = pod;
            this.ready = ready;
            if (reasonsToRestart != null) {
                this.reasonsToRestart.addAll(reasonsToRestart);
            }
        }

        String getPodName() {
            return pod.getMetadata().getName();
        }

        boolean requiresRestart() {
            return !reasonsToRestart.isEmpty();
        }
    }
}