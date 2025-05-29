/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatus;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;

import java.time.Instant;
import java.util.Map;


/**
 * Utility class for updating data in KafkaRebalance ConfigMap.
 */
public class KafkaRebalanceConfigMapUtils {
    /**
     * The estimated time it will take in minutes until the partition rebalance is complete rounded
     * to the nearest minute.
     */
    /* test */ static final String ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY = "estimatedTimeToCompletionInMinutes";
    /**
     *  The percentage of the byte movement of the partition rebalance that is completed as a rounded down integer
     *  value in the range [0-100].
     */
    /* test */ static final String COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY = "completedByteMovementPercentage";
    /**
     * The “non-verbose” JSON payload from the "/kafkacruisecontrol/state?substates=executor" endpoint,
     * providing details about the executor's current status, including partition movement progress,
     * concurrency limits, and total data to move.
     */
    /* test */ static final String EXECUTOR_STATE_KEY = "executorState.json";

    /* test */ static final String TIME_COMPLETED = "0";
    /* test */ static final String BYTE_MOVEMENT_ZERO = "0";
    /* test */ static final String BYTE_MOVEMENT_COMPLETED = "100";

    /**
     * Updates the given KafkaRebalance ConfigMap with progress fields based on the progress of the Kafka rebalance operation.
     *
     * @param state          The current state of the KafkaRebalance resource (e.g., ProposalReady, Rebalancing, Stopped, etc.).
     * @param executorStatus The executor status information of executing task, which is used to calculate progress fields.
     *                       in the Rebalancing state.
     * @param configMap      The ConfigMap to be updated with progress information.
     */
    /* test */ static void updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState state,
                                                                      ExecutorStatus executorStatus,
                                                                      ConfigMap configMap) {
        if (configMap == null || configMap.getData() == null) {
            return;
        }
        Map<String, String> data = configMap.getData();

        switch (state) {
            case ProposalReady:
                data.remove(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY);
                data.put(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY, BYTE_MOVEMENT_ZERO);
                data.remove(EXECUTOR_STATE_KEY);
                break;
            case Rebalancing:
                Instant taskStartTime = executorStatus.getTaskStartTime();
                int totalDataToMove = executorStatus.getTotalDataToMove();
                int finishedDataMovement = executorStatus.getFinishedDataMovement();

                int estimatedTimeToCompletion = KafkaRebalanceProgressUtils.estimateTimeToCompletionInMinutes(
                        taskStartTime,
                        totalDataToMove,
                        finishedDataMovement);

                int completedByteMovement = KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(
                        totalDataToMove,
                        finishedDataMovement);

                data.put(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY, String.valueOf(estimatedTimeToCompletion));
                data.put(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY, String.valueOf(completedByteMovement));
                data.put(EXECUTOR_STATE_KEY, executorStatus.getJson().toString());
                break;
            case Stopped:
            case NotReady:
                data.remove(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY);
                // Use the value of completedByteMovementPercentage from previous update.
                // Use the value of executorStateJson object from previous update.
                break;
            case Ready:
                data.put(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY, TIME_COMPLETED);
                data.put(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY, BYTE_MOVEMENT_COMPLETED);
                data.remove(EXECUTOR_STATE_KEY);
                break;
            case New:
            case PendingProposal:
            default:
                data.remove(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY);
                data.remove(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY);
                data.remove(EXECUTOR_STATE_KEY);
                break;
        }
    }

    /**
     * Updates the KafkaRebalance {@link ConfigMap} with relevant state information depending on the current
     * {@link KafkaRebalanceState}.
     *
     * @param reconciliation The reconciliation context.
     * @param status         The KafkaRebalance status.
     * @param host           The host address of the Cruise Control instance.
     * @param port           The port of the Cruise Control instance.
     * @param apiClient      The API client to communicate with Cruise Control.
     * @param configMap      The desired ConfigMap.
     * @return A {@link Future} representing the updated ConfigMap (or null if no update was required).
     */
    public static Future<ConfigMap> updateRebalanceConfigMap(Reconciliation reconciliation,
                                                             KafkaRebalanceStatus status,
                                                             String host,
                                                             int port,
                                                             CruiseControlApi apiClient,
                                                             ConfigMap configMap) {
        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(status);
        if (state == KafkaRebalanceState.Rebalancing) {
            return VertxUtil.completableFutureToVertxFuture(
                    apiClient.getCruiseControlState(reconciliation, host, port, false))
                    .compose(response -> {
                        ExecutorStatus executorStatus = response.getExecutorStatus();
                        if (executorStatus.isInProgressState()) {
                            // We can only estimate the rate of partition movement once some data has been moved.
                            if (executorStatus.getFinishedDataMovement() > 0) {
                                updateRebalanceConfigMapWithProgressFields(state, executorStatus, configMap);
                                return Future.succeededFuture(configMap);
                            }
                        }
                        throw new IllegalStateException(
                                String.format("Partition movement information unavailable; executor is in '%s' state, " +
                                        "progress estimation will be updated on next reconciliation.", executorStatus.getState()));
                    });
        } else {
            updateRebalanceConfigMapWithProgressFields(state, null, configMap);
            return Future.succeededFuture(configMap);
        }
    }
}
