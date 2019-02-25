/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.function.Predicate;


/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Zookeeper nodes
 */
public class ZookeeperSetOperator extends StatefulSetOperator {

    private static final Logger log = LogManager.getLogger(ZookeeperSetOperator.class);
    private final ZookeeperLeaderFinder leaderFinder;

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public ZookeeperSetOperator(Vertx vertx, KubernetesClient client, ZookeeperLeaderFinder leaderFinder, long operationTimeoutMs) {
        super(vertx, client, operationTimeoutMs);
        this.leaderFinder = leaderFinder;
    }

    @Override
    protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
        StatefulSetDiff diff = new StatefulSetDiff(current, desired);
        if (diff.changesVolumeClaimTemplates()) {
            log.warn("Changing Zookeeper storage type or size is not possible. The changes will be ignored.");
            diff = revertStorageChanges(current, desired);
        }
        return !diff.isEmpty() && needsRollingUpdate(diff);
    }

    public static boolean needsRollingUpdate(StatefulSetDiff diff) {
        // Because for ZK the brokers know about each other via the config, and rescaling requires a rolling update
        if (diff.changesSpecReplicas()) {
            log.debug("Changed #replicas => needs rolling update");
            return true;
        }
        if (diff.changesLabels()) {
            log.debug("Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplateSpec()) {
            log.debug("Changed template spec => needs rolling update");
            return true;
        }
        return false;
    }

    /**
     * Method `maybeRollingUpdate` uses an algorithm for rolling update of Zookeeper cluster.
     * It is based on restarting the Zookeeper leader replica as the last one. So the quorum is preserved.
     * The leader is determined by sending `stat` word to each pod.
     */
    @Override
    public Future<Void> maybeRollingUpdate(StatefulSet ss, Predicate<Pod> podRestart) {
        String cluster = ss.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        return maybeRollingUpdate(ss, podRestart, leaderFinder.secretOperator.get(ss.getMetadata().getNamespace(),
                ClusterOperator.secretName(cluster)));
    }

    public Future<Void> maybeRollingUpdate(StatefulSet ss, Predicate<Pod> podRestart, Secret coKeySecret) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        final int replicas = ss.getSpec().getReplicas();
        log.debug("Considering rolling update of {}/{}", namespace, name);

        boolean zkRoll = false;
        ArrayList<Pod> pods = new ArrayList<>();
        String cluster = ss.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        for (int i = 0; i < replicas; i++) {
            Pod pod = podOperations.get(ss.getMetadata().getNamespace(), KafkaResources.zookeeperPodName(cluster, i));
            zkRoll |= podRestart.test(pod);
            pods.add(pod);
        }

        final Future<Void> rollFuture;
        if (zkRoll) {
            // Find the leader
            rollFuture = Future.future();
            Future<Integer> leaderFuture = leaderFinder.findZookeeperLeader(cluster, namespace, pods, coKeySecret);
            leaderFuture.compose(leader -> {
                log.debug("Zookeeper leader is " + (leader == ZookeeperLeaderFinder.UNKNOWN_LEADER ? "unknown" : "pod " + leader));
                Future<Void> fut = Future.succeededFuture();
                // Then roll each non-leader pod
                for (int i = 0; i < replicas; i++) {
                    String podName = KafkaResources.zookeeperPodName(cluster, i);
                    if (i != leader) {
                        log.debug("Possibly restarting non-leader pod {}", podName);
                        // roll the pod and wait until it is ready
                        // this prevents rolling into faulty state (note: this applies just for ZK pods)
                        fut = fut.compose(ignore -> maybeRestartPod(ss, podName, podRestart));
                    } else {
                        log.debug("Deferring restart of leader {}", podName);
                    }
                }
                if (leader == ZookeeperLeaderFinder.UNKNOWN_LEADER) {
                    return fut;
                } else {
                    // Finally roll the leader pod
                    return fut.compose(ar -> {
                        // the leader is rolled as the last
                        log.debug("Possibly restarting leader pod (previously deferred) {}", leader);
                        return maybeRestartPod(ss, KafkaResources.zookeeperPodName(cluster, leader), podRestart);
                    });
                }
            }).setHandler(rollFuture.completer());
        } else {
            rollFuture = Future.succeededFuture();
        }
        return rollFuture;
    }

}
