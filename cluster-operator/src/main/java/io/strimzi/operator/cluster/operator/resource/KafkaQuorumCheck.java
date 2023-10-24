/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.QuorumInfo;

import static java.lang.Math.ceil;

/**
 * Provides a method that determines whether it's safe to restart a KRaft controller
 */
class KafkaQuorumCheck {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaQuorumCheck.class.getName());
    private final Reconciliation reconciliation;
    private final Admin admin;
    private final Vertx vertx;
    private final long controllerQuorumFetchTimeoutMs;

    KafkaQuorumCheck(Reconciliation reconciliation, Admin ac, Vertx vertx, long controllerQuorumFetchTimeoutMs) {
        this.reconciliation = reconciliation;
        this.admin = ac;
        this.vertx = vertx;
        this.controllerQuorumFetchTimeoutMs = controllerQuorumFetchTimeoutMs;
    }

    /**
     * Returns future that completes with true if the given controller can be rolled based on the quorum state. Quorum is considered
     * healthy if the majority of controllers have caught up with the quorum leader within the controller.quorum.fetch.timeout.ms.
     */
    Future<Boolean> canRollController(int podId) {
        LOGGER.debugCr(reconciliation, "Determining whether controller {} can be rolled", podId);
        return describeMetadataQuorum().map(info -> {
            boolean canRoll = isQuorumHealthyWithoutPod(podId, info);
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Restart pod {} would affect the quorum", podId);
            }
            return canRoll;
        }).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart pod {}", podId, error);
            return Future.failedFuture(error);
        });
    }

    /**
     * Returns id of the quorum leader.
     **/
    Future<Integer> quorumLeaderId() {
        LOGGER.debugCr(reconciliation, "Determining the quorum leader id");
        return describeMetadataQuorum().map(QuorumInfo::leaderId).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining the quorum leader id", error);
            return Future.failedFuture(error);
        });
    }

    private boolean isQuorumHealthyWithoutPod(int podId, QuorumInfo info) {
        int leaderId = info.leaderId();
        if (leaderId < 0) {
            LOGGER.warnCr(reconciliation, "No quorum leader is found because the leader id is set to {}", leaderId);
            return false;
        }
        Map<Integer, Long> replicaStates = info.voters().stream().collect(Collectors.toMap(
                QuorumInfo.ReplicaState::replicaId,
                replicaState -> replicaState.lastCaughtUpTimestamp().isPresent() ? replicaState.lastCaughtUpTimestamp().getAsLong() : -1));
        int totalNumOfControllers = replicaStates.size();
        //cannot use normal integer as it's being incremented inside the lambda expression
        AtomicInteger numOfCaughtUpControllers = new AtomicInteger();
        long leaderLastCaughtUpTimestamp = replicaStates.get(leaderId);
        LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for the leader replica {} is {}", leaderId, leaderLastCaughtUpTimestamp);
        replicaStates.forEach((replicaId, lastCaughtUpTimestamp) -> {
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for the replica {} ", replicaId);
            } else {
                LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for controller {} is {}", replicaId, lastCaughtUpTimestamp);
                if (replicaId == leaderId || (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp) < controllerQuorumFetchTimeoutMs) {
                    // skip the controller that we are considering to roll
                    if (replicaId != podId) {
                        numOfCaughtUpControllers.getAndIncrement();
                    }
                    LOGGER.debugCr(reconciliation, "The controller {} has caught up with the quorum leader", replicaId);
                } else {
                    LOGGER.debugCr(reconciliation, "The controller {} has fallen behind the leader", replicaId);
                }
            }
        });
        LOGGER.debugCr(reconciliation, "Out of {} voters, there are {} controllers that have caught up with the quorum leader, not including the controller {}", totalNumOfControllers, numOfCaughtUpControllers, podId);
        return numOfCaughtUpControllers.get() >= ceil((double) (totalNumOfControllers + 1) / 2);
    }

    private Future<QuorumInfo> describeMetadataQuorum() {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, admin.describeMetadataQuorum().quorumInfo());
    }
}
