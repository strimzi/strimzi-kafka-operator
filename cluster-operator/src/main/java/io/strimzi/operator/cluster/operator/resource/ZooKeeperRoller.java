/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Labels;
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
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZooKeeperRoller.class.getName());
    private static final long READINESS_POLLING_INTERVAL_MS = 1_000;

    private final PodOperator podOperator;
    private final ZookeeperLeaderFinder leaderFinder;
    private final long operationTimeoutMs;

    /**
     * Constructor
     *
     * @param podOperator        Pod operator
     * @param leaderFinder       ZooKeeper Leader Finder
     * @param operationTimeoutMs Operation timeout in milliseconds
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
     * @param replicas          Number of ZooKeeper replicas to roll
     * @param selectorLabels    The selector labels to find the pods
     * @param podRestart        Function that returns a list is reasons why the given pod needs to be restarted, or an
     *                          empty list if the pod does not need to be restarted.
     * @param coTlsPemIdentity  Trust set and identity for TLS client authentication for connecting to ZooKeeper
     *
     * @return A future that completes when any necessary rolling has been completed.
     */
    public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, int replicas, Labels selectorLabels, Function<Pod, List<String>> podRestart, TlsPemIdentity coTlsPemIdentity) {
        String namespace = reconciliation.namespace();

        // We prepare the list of expected Pods. This is needed as we need to account for pods which might be missing.
        // We need to wait for them before rolling any running pods to avoid problems.
        List<String> expectedPodNames = new ArrayList<>();
        for (int i = 0; i < replicas; i++)  {
            expectedPodNames.add(KafkaResources.zookeeperPodName(reconciliation.name(), i));
        }

        return podOperator.listAsync(namespace, selectorLabels)
                .compose(pods -> {
                    ZookeeperClusterRollContext clusterRollContext = new ZookeeperClusterRollContext();

                    for (String podName : expectedPodNames) {
                        Pod pod = pods.stream().filter(p -> podName.equals(p.getMetadata().getName())).findFirst().orElse(null);

                        if (pod != null)    {
                            List<String> restartReasons = podRestart.apply(pod);
                            final boolean ready = podOperator.isReady(namespace, pod.getMetadata().getName());
                            ZookeeperPodContext podContext = new ZookeeperPodContext(podName, restartReasons, true, ready);
                            if (restartReasons != null && !restartReasons.isEmpty())    {
                                LOGGER.debugCr(reconciliation, "Pod {} should be rolled due to {}", podContext.getPodName(), restartReasons);
                            } else {
                                LOGGER.debugCr(reconciliation, "Pod {} does not need to be rolled", podContext.getPodName());
                            }
                            clusterRollContext.add(podContext);
                        } else {
                            // Pod does not exist, but we still add it to the roll context because we should not roll
                            // any other pods before it is ready
                            LOGGER.debugCr(reconciliation, "Pod {} does not exist and cannot be rolled", podName);
                            ZookeeperPodContext podContext = new ZookeeperPodContext(podName, null, false, false);
                            clusterRollContext.add(podContext);
                        }
                    }

                    if (clusterRollContext.requiresRestart())   {
                        return Future.succeededFuture(clusterRollContext);
                    } else {
                        return Future.succeededFuture(null);
                    }
                }).compose(clusterRollContext -> {
                    if (clusterRollContext != null)  {
                        Promise<Void> promise = Promise.promise();
                        Future<String> leaderFuture = leaderFinder.findZookeeperLeader(reconciliation, clusterRollContext.podNames(), coTlsPemIdentity);

                        leaderFuture.compose(leader -> {
                            LOGGER.debugCr(reconciliation, "Zookeeper leader is " + (ZookeeperLeaderFinder.UNKNOWN_LEADER.equals(leader) ? "unknown" : "pod " + leader));
                            Future<Void> fut = Future.succeededFuture();

                            // Then roll each non-leader pod => the leader is rolled last
                            for (ZookeeperPodContext podContext : clusterRollContext.getPodContextsWithNonExistingAndNonReadyFirst())  {
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
        LOGGER.infoCr(reconciliation, "Rolling Pod {} due to {}", podName, reasons);
        return podOperator.getAsync(reconciliation.namespace(), podName)
                .compose(pod -> podOperator.restart(reconciliation, pod, operationTimeoutMs))
                .compose(ignore -> {
                    LOGGER.debugCr(reconciliation, "Waiting for readiness of pod {}", podName);
                    return podOperator.readiness(reconciliation, reconciliation.namespace(), podName, READINESS_POLLING_INTERVAL_MS, operationTimeoutMs);
                });
    }

    /**
     * Internal class which helps to establish which pods need to be rolled and what should be the rolling order
     */
    /* test */ static class ZookeeperClusterRollContext {
        private final List<ZookeeperPodContext> podContexts = new ArrayList<>();

        /**
         * Constructor
         */
        ZookeeperClusterRollContext() {
        }

        /**
         * @return  List of pods to consider for rolling in the right order -> missing pods first, unready next, ready last.
         */
        List<ZookeeperPodContext> getPodContextsWithNonExistingAndNonReadyFirst() {
            return podContexts.stream().sorted(ZookeeperClusterRollContext::findNext).collect(Collectors.toList());
        }

        /**
         * Utility method to help order the pods in the order in which they should be checked for rolling. It is used as
         * a comparator to compere to ZooKeeperPodContext instances. It compares them in the way that missing pods are
         * checked first, then unready pods and only at the end the ready pods.
         *
         * @param contextA  Context for Pod A
         * @param contextB  Context for Pod B
         *
         * @return  -1 if the Pod from contextA should be rolled first, 0 when their equal in their rolling order and 1
         *          when the Pod from contextB should be checked first.
         */
        private static int findNext(ZookeeperPodContext contextA, ZookeeperPodContext contextB)  {
            if (!contextA.exists && !contextB.exists)   {
                return 0;
            } else if (!contextA.exists)   {
                return -1;
            } else if (!contextB.exists)   {
                return 1;
            } else {
                return Boolean.compare(contextA.ready, contextB.ready);
            }
        }

        /**
         * Add a ZooKeeper Pod to the rolling context
         *
         * @param podContext    ZooKeeper Pod context representing a pod which should be rolled or considered for rolling
         */
        void add(final ZookeeperPodContext podContext) {
            podContexts.add(podContext);
        }

        /**
         * @return  True if any of the pods in this context requires restart. False otherwise.
         */
        boolean requiresRestart() {
            return podContexts.stream().anyMatch(ZookeeperPodContext::requiresRestart);
        }

        /**
         * @return  Set with the names of the ZooKeeper pods in this context
         */
        Set<String> podNames() {
            return podContexts.stream().map(ZookeeperPodContext::getPodName).collect(Collectors.toSet());
        }

        /**
         * Gets a Pod context for a given pod name
         *
         * @param podName   Name of the pod for which we want to retrieve the context
         *
         * @return  The ZooKeeper Pod Context
         */
        ZookeeperPodContext get(final String podName) {
            return podContexts.stream().filter(podContext -> podContext.getPodName().equals(podName)).findAny().orElse(null);
        }
    }

    /**
     * Internal class which carries the rolling context for a specific ZooKeeper Pod
     */
    /* test */ static class ZookeeperPodContext {
        private final String podName;
        private final boolean exists;
        private final boolean ready;
        private final List<String> reasonsToRestart = new ArrayList<>();

        /**
         * Constructs the ZooKeeper Pod Context
         *
         * @param podName          Name of this ZooKeeper pod
         * @param reasonsToRestart List with the reasons why this pod might need to be restarted
         * @param exists           Flag indicating if this pod exists or not
         * @param ready            Flag indicating whether this pod is ready or not
         */
        ZookeeperPodContext(final String podName, final List<String> reasonsToRestart, final boolean exists, final boolean ready) {
            this.podName = podName;
            this.exists = exists;
            this.ready = ready;

            if (reasonsToRestart != null) {
                this.reasonsToRestart.addAll(reasonsToRestart);
            }
        }

        /**
         * @return Name of this pod
         */
        String getPodName() {
            return podName;
        }

        /**
         * @return True if this pod requires restart. False otherwise.
         */
        boolean requiresRestart() {
            return !reasonsToRestart.isEmpty();
        }
    }
}