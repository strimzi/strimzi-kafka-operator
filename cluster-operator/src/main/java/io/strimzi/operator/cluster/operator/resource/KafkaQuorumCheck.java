/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.QuorumInfo;

/**
 * Determines whether the given controller can be rolled without affecting the quorum.
 */
class KafkaQuorumCheck {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaQuorumCheck.class.getName());
    private final Admin ac;
    private final Reconciliation reconciliation;
    private final Future<QuorumInfo> quorumInfoFuture;
    private final String controllerQuorumFetchTimeoutMs;

    KafkaQuorumCheck(Reconciliation reconciliation, Admin ac, String controllerQuorumFetchTimeoutMs) {
        this.ac = ac;
        this.reconciliation = reconciliation;
        this.quorumInfoFuture = describeMetadataQuorum();
        this.controllerQuorumFetchTimeoutMs = controllerQuorumFetchTimeoutMs;
    }

    /**
     * Determine whether the given controller can be rolled without affecting the quorum
     */
    Future<Boolean> canRoll(int podId) {
        LOGGER.debugCr(reconciliation, "Determining whether controller {} can be rolled", podId);
        return canRollController(quorumInfoFuture, podId);
    }

    private Future<Boolean> canRollController(Future<QuorumInfo> quorumInfo, int podId) {
        return quorumInfo.map(info -> {
            int leaderId = info.leaderId();
            Map<Integer, Long> replicaIdToLastCaughtUpTimestampMap = info.voters().stream().collect(Collectors.toMap(
                    QuorumInfo.ReplicaState::replicaId,
                    replicaState -> replicaState.lastCaughtUpTimestamp().isPresent() ? replicaState.lastCaughtUpTimestamp().getAsLong() : -1));
            boolean canRoll = wouldAffectQuorum(leaderId, replicaIdToLastCaughtUpTimestampMap);
            if (!canRoll) {
                LOGGER.debugCr(reconciliation, "Restart pod {} would affect the quorum", podId);
            }
            return canRoll;
        }).recover(error -> {
            LOGGER.warnCr(reconciliation, "Error determining whether it is safe to restart pod {}", podId, error);
            return Future.failedFuture(error);
        });
    }

    private boolean wouldAffectQuorum(int leaderId, Map<Integer, Long> replicaStates) {
        int totalNumVoters = replicaStates.size();
        AtomicInteger numOfCaughtUpVoters = new AtomicInteger();
        long leaderLastCaughtUpTimestamp = replicaStates.get(leaderId);
        if (leaderLastCaughtUpTimestamp < 0) {
            LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for the leader replica {} ", leaderId);
            return false;
        }
        LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for the leader replica {} is {}", leaderId, leaderLastCaughtUpTimestamp);
        replicaStates.forEach((replicaId, lastCaughtUpTimestamp) -> {
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.warnCr(reconciliation, "No valid lastCaughtUpTimestamp is found for the replica {} ", replicaId);
            } else {
                if (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp < Long.parseLong(controllerQuorumFetchTimeoutMs)) {
                    numOfCaughtUpVoters.getAndIncrement();
                    LOGGER.debugCr(reconciliation, "The replica {} has caught up with the leader", replicaId);
                    LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for replica {} is {}", replicaId, lastCaughtUpTimestamp);
                } else {
                    LOGGER.debugCr(reconciliation, "The replica {} is behind from the leader", replicaId);
                }
            }
        });
        LOGGER.infoCr(reconciliation, "Out of {} voters, {} followers have caught up with the leader", totalNumVoters, numOfCaughtUpVoters);
        return numOfCaughtUpVoters.get() > (totalNumVoters / 2);
    }

    protected Future<QuorumInfo> describeMetadataQuorum() {
        Promise<QuorumInfo> descPromise = Promise.promise();
        ac.describeMetadataQuorum().quorumInfo().whenComplete(
                ((quorumInfo, error) -> {
                    if (error != null) {
                        descPromise.fail(error);
                    } else {
                        descPromise.complete(quorumInfo);
                    }
                })
        );
        return descPromise.future();
    }
}
