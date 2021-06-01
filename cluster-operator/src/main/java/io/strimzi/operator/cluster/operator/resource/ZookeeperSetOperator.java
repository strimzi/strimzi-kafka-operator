/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Zookeeper nodes
 */
public class ZookeeperSetOperator extends StatefulSetOperator {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZookeeperSetOperator.class);
    private final ZookeeperLeaderFinder leaderFinder;

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     * @param leaderFinder The Zookeeper leader finder.
     * @param operationTimeoutMs The timeout.
     */
    public ZookeeperSetOperator(Vertx vertx, KubernetesClient client, ZookeeperLeaderFinder leaderFinder, long operationTimeoutMs) {
        super(vertx, client, operationTimeoutMs);
        this.leaderFinder = leaderFinder;
    }

    @Override
    protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
        return !diff.isEmpty() && needsRollingUpdate(reconciliation, diff);
    }

    public static boolean needsRollingUpdate(Reconciliation reconciliation, StatefulSetDiff diff) {
        if (diff.changesLabels()) {
            LOGGER.debugCr(reconciliation, "Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplate()) {
            LOGGER.debugCr(reconciliation, "Changed template spec => needs rolling update");
            return true;
        }
        if (diff.changesVolumeClaimTemplates()) {
            LOGGER.debugCr(reconciliation, "Changed volume claim template => needs rolling update");
            return true;
        }
        if (diff.changesVolumeSize()) {
            LOGGER.debugCr(reconciliation, "Changed size of the volume claim template => no need for rolling update");
            return false;
        }
        return false;
    }

    @Override
    public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podRestart, Secret clusterCaSecret, Secret coKeySecret) {
        String namespace = sts.getMetadata().getNamespace();
        String name = sts.getMetadata().getName();
        final int replicas = sts.getSpec().getReplicas();
        LOGGER.debugCr(reconciliation, "Considering rolling update of {}/{}", namespace, name);

        boolean zkRoll = false;
        ArrayList<Pod> pods = new ArrayList<>(replicas);
        String cluster = sts.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        for (int i = 0; i < replicas; i++) {
            Pod pod = podOperations.get(sts.getMetadata().getNamespace(), KafkaResources.zookeeperPodName(cluster, i));
            List<String> zkPodRestart = podRestart.apply(pod);
            zkRoll |= zkPodRestart != null && !zkPodRestart.isEmpty();
            pods.add(pod);
        }

        final Future<Void> rollFuture;
        if (zkRoll) {
            // Find the leader
            Promise<Void> promise = Promise.promise();
            rollFuture = promise.future();
            Future<Integer> leaderFuture = leaderFinder.findZookeeperLeader(cluster, namespace, pods, coKeySecret);
            leaderFuture.compose(leader -> {
                LOGGER.debugCr(reconciliation, "Zookeeper leader is " + (leader == ZookeeperLeaderFinder.UNKNOWN_LEADER ? "unknown" : "pod " + leader));
                Future<Void> fut = Future.succeededFuture();
                // Then roll each non-leader pod
                for (int i = 0; i < replicas; i++) {
                    String podName = KafkaResources.zookeeperPodName(cluster, i);
                    if (i != leader) {
                        LOGGER.debugCr(reconciliation, "Possibly restarting non-leader pod {}", podName);
                        // roll the pod and wait until it is ready
                        // this prevents rolling into faulty state (note: this applies just for ZK pods)
                        fut = fut.compose(ignore -> maybeRestartPod(reconciliation, sts, podName, podRestart));
                    } else {
                        LOGGER.debugCr(reconciliation, "Deferring restart of leader {}", podName);
                    }
                }
                if (leader == ZookeeperLeaderFinder.UNKNOWN_LEADER) {
                    return fut;
                } else {
                    // Finally roll the leader pod
                    return fut.compose(ar -> {
                        // the leader is rolled as the last
                        LOGGER.debugCr(reconciliation, "Possibly restarting leader pod (previously deferred) {}", leader);
                        return maybeRestartPod(reconciliation, sts, KafkaResources.zookeeperPodName(cluster, leader), podRestart);
                    });
                }
            }).onComplete(promise);
        } else {
            rollFuture = Future.succeededFuture();
        }
        return rollFuture;
    }

}
