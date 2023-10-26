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

    protected KafkaQuorumCheck(Reconciliation reconciliation, Admin ac, Vertx vertx, long controllerQuorumFetchTimeoutMs) {
        this.reconciliation = reconciliation;
        this.admin = ac;
        this.vertx = vertx;
        this.controllerQuorumFetchTimeoutMs = controllerQuorumFetchTimeoutMs;
    }

    /**
     * Returns future that completes with true if the given controller can be rolled based on the quorum state. Quorum is considered
     * healthy if the majority of controllers have caught up with the quorum leader within the controller.quorum.fetch.timeout.ms.
     */
    Future<Boolean> canRollController(int nodeId) {
        LOGGER.debugCr(reconciliation, "Determining whether controller pod {} can be rolled", nodeId);
        return describeMetadataQuorum().map(info -> {
            boolean canRoll = isQuorumHealthyWithoutNode(nodeId, info);
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Not restarting controller pod {}. Restart would affect the quorum health", nodeId);
            }
            return canRoll;
        }).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart controller pod {}", nodeId, error);
            return Future.failedFuture(error);
        });
    }

    /**
     * Returns id of the quorum leader.
     **/
    Future<Integer> quorumLeaderId() {
        LOGGER.debugCr(reconciliation, "Determining the controller quorum leader id");
        return describeMetadataQuorum().map(QuorumInfo::leaderId).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining the controller quorum leader id", error);
            return Future.failedFuture(error);
        });
    }

    private boolean isQuorumHealthyWithoutNode(int nodeId, QuorumInfo info) {
        int leaderId = info.leaderId();
        if (leaderId < 0) {
            LOGGER.warnCr(reconciliation, "No controller quorum leader is found because the leader id is set to {}", leaderId);
            return false;
        }
        Map<Integer, Long> controllerStates = info.voters().stream().collect(Collectors.toMap(
                QuorumInfo.ReplicaState::replicaId,
                state -> state.lastCaughtUpTimestamp().isPresent() ? state.lastCaughtUpTimestamp().getAsLong() : -1));
        int totalNumOfControllers = controllerStates.size();
        if (totalNumOfControllers == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with a single node. The cluster may be " +
                    "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
            return true;
        }
        //cannot use normal integer as it's being incremented inside the lambda expression
        AtomicInteger numOfCaughtUpControllers = new AtomicInteger();
        long leaderLastCaughtUpTimestamp = controllerStates.get(leaderId);
        LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for the controller quorum leader (node id {}) is {}", leaderId, leaderLastCaughtUpTimestamp);
        controllerStates.forEach((controllerNodeId, lastCaughtUpTimestamp) -> {
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for controller {} ", controllerNodeId);
            } else {
                LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for controller {} is {}", controllerNodeId, lastCaughtUpTimestamp);
                if (controllerNodeId == leaderId || (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp) < controllerQuorumFetchTimeoutMs) {
                    // skip the controller that we are considering to roll
                    if (controllerNodeId != nodeId) {
                        numOfCaughtUpControllers.getAndIncrement();
                    }
                    LOGGER.debugCr(reconciliation, "Controller {} has caught up with the controller quorum leader", controllerNodeId);
                } else {
                    LOGGER.debugCr(reconciliation, "Controller {} has fallen behind the controller quorum leader", controllerNodeId);
                }
            }
        });
        LOGGER.debugCr(reconciliation, "Out of {} controllers, there are {} that have caught up with the controller quorum leader, not including controller {}", totalNumOfControllers, numOfCaughtUpControllers, nodeId);
        if (totalNumOfControllers == 2) {
            // Only roll the controller if the other one in the quorum has caught up or is the active controller.
            if (numOfCaughtUpControllers.get() == 1) {
                LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with 2 nodes. The cluster may be " +
                        "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
                return true;
            } else {
                return false;
            }
        } else {
            return numOfCaughtUpControllers.get() >= ceil((double) (totalNumOfControllers + 1) / 2);
        }
    }

    private Future<QuorumInfo> describeMetadataQuorum() {
        return VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, admin.describeMetadataQuorum().quorumInfo());
    }
}
