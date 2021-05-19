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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Zookeeper nodes
 */
public class ZookeeperSetOperator extends StatefulSetOperator {

    private static final Logger LOGGER = LogManager.getLogger(ZookeeperSetOperator.class);
    private static final ReconciliationLogger RECONCILIATION_LOGGER = ReconciliationLogger.create(LOGGER);
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
            RECONCILIATION_LOGGER.debug(reconciliation, "Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplate()) {
            RECONCILIATION_LOGGER.debug(reconciliation, "Changed template spec => needs rolling update");
            return true;
        }
        if (diff.changesVolumeClaimTemplates()) {
            RECONCILIATION_LOGGER.debug(reconciliation, "Changed volume claim template => needs rolling update");
            return true;
        }
        if (diff.changesVolumeSize()) {
            RECONCILIATION_LOGGER.debug(reconciliation, "Changed size of the volume claim template => no need for rolling update");
            return false;
        }
        return false;
    }

    @Override
    public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podRestart, Secret clusterCaSecret, Secret coKeySecret) {
        String namespace = sts.getMetadata().getNamespace();
        String name = sts.getMetadata().getName();
        final int replicas = sts.getSpec().getReplicas();
        RECONCILIATION_LOGGER.debug(reconciliation, "Considering rolling update of {}/{}", namespace, name);

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
            Future<Integer> leaderFuture = leaderFinder.findZookeeperLeader(reconciliation, cluster, namespace, pods, coKeySecret);
            leaderFuture.compose(leader -> {
                RECONCILIATION_LOGGER.debug(reconciliation, "Zookeeper leader is " + (leader == ZookeeperLeaderFinder.UNKNOWN_LEADER ? "unknown" : "pod " + leader));
                Future<Void> fut = Future.succeededFuture();
                // Then roll each non-leader pod
                for (int i = 0; i < replicas; i++) {
                    String podName = KafkaResources.zookeeperPodName(cluster, i);
                    if (i != leader) {
                        RECONCILIATION_LOGGER.debug(reconciliation, "Possibly restarting non-leader pod {}", podName);
                        // roll the pod and wait until it is ready
                        // this prevents rolling into faulty state (note: this applies just for ZK pods)
                        fut = fut.compose(ignore -> maybeRestartPod(reconciliation, sts, podName, podRestart));
                    } else {
                        RECONCILIATION_LOGGER.debug(reconciliation, "Deferring restart of leader {}", podName);
                    }
                }
                if (leader == ZookeeperLeaderFinder.UNKNOWN_LEADER) {
                    return fut;
                } else {
                    // Finally roll the leader pod
                    return fut.compose(ar -> {
                        // the leader is rolled as the last
                        RECONCILIATION_LOGGER.debug(reconciliation, "Possibly restarting leader pod (previously deferred) {}", leader);
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
