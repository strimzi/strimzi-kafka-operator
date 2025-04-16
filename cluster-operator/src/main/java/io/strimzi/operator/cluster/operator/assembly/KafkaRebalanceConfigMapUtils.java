/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.model.cruisecontrol.ExecutorStateProcessor;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;

import java.time.Instant;
import java.util.Map;

/**
 * Utility class for updating data in KafkaRebalance ConfigMap
 */
public class KafkaRebalanceConfigMapUtils {

    /* test */ static final String REBALANCE_PROGRESS_CONFIG_MAP_KEY = "rebalanceProgressConfigMap";
    /* test */ static final String ESTIMATED_TIME_TO_COMPLETION_KEY = "estimatedTimeToCompletionInMinutes";
    /* test */ static final String COMPLETED_BYTE_MOVEMENT_KEY = "completedByteMovementPercentage";
    /* test */ static final String EXECUTOR_STATE_KEY = "executorState.json";

    /* test */ static final String TIME_COMPLETED = "0";
    /* test */ static final String BYTE_MOVEMENT_ZERO = "0";
    /* test */ static final String BYTE_MOVEMENT_COMPLETED = "100";

    /**
     * Updates the given KafkaRebalance ConfigMap with progress fields based on the progress of the Kafka rebalance operation.
     *
     * @param state The current state of the KafkaRebalance resource (e.g., ProposalReady, Rebalancing, Stopped, etc.).
     * @param executorState The executor state information in JSON format, which is used to calculate progress fields
     *                      in the Rebalancing state.
     * @param configMap The ConfigMap to be updated with progress information.
     */
    /* test */ static void updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState state, JsonNode executorState, ConfigMap configMap) {
        Map<String, String> data = configMap != null ? configMap.getData() : null;

        switch (state) {
            case ProposalReady:
                data.remove(ESTIMATED_TIME_TO_COMPLETION_KEY);
                data.put(COMPLETED_BYTE_MOVEMENT_KEY, BYTE_MOVEMENT_ZERO);
                data.remove(EXECUTOR_STATE_KEY);
                break;
            case Rebalancing:
                int taskStartTime = ExecutorStateProcessor.getTaskStartTime(executorState);
                int totalDataToMove = ExecutorStateProcessor.getTotalDataToMove(executorState);
                int finishedDataMovement = ExecutorStateProcessor.getFinishedDataMovement(executorState);

                int estimatedTimeToCompletion = KafkaRebalanceProgressUtils.estimateTimeToCompletionInMinutes(
                        taskStartTime,
                        Instant.now().getEpochSecond(),
                        totalDataToMove,
                        finishedDataMovement);

                int completedByteMovement = KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(
                        totalDataToMove,
                        finishedDataMovement);

                data.put(ESTIMATED_TIME_TO_COMPLETION_KEY, String.valueOf(estimatedTimeToCompletion));
                data.put(COMPLETED_BYTE_MOVEMENT_KEY, String.valueOf(completedByteMovement));
                data.put(EXECUTOR_STATE_KEY, executorState.toString());
                break;
            case Stopped:
            case NotReady:
                data.remove(ESTIMATED_TIME_TO_COMPLETION_KEY);
                // Use the value of completedByteMovementPercentage from previous update.
                // Use the value of executorState object from previous update.
                break;
            case Ready:
                data.put(ESTIMATED_TIME_TO_COMPLETION_KEY, TIME_COMPLETED);
                data.put(COMPLETED_BYTE_MOVEMENT_KEY, BYTE_MOVEMENT_COMPLETED);
                data.remove(EXECUTOR_STATE_KEY);
                break;
            default:
                break;
        }
    }

    /**
     * Updates the KafkaRebalance {@link ConfigMap} with relevant state information depending on the current
     * {@link KafkaRebalanceState}.
     *
     * @param reconciliation The reconciliation context
     * @param state The KafkaRebalance state
     * @param host The host address of the Cruise Control instance
     * @param port The port of the Cruise Control instance
     * @param apiClient The API client to communicate with Cruise Control
     * @param configMap The desired ConfigMap
     *
     * @return A {@link Future} representing the updated ConfigMap (or null if no update was required)
     */
    public static Future<ConfigMap> updateRebalanceConfigMap(Reconciliation reconciliation,
                                                      KafkaRebalanceState state,
                                                      String host,
                                                      int port,
                                                      CruiseControlApi apiClient,
                                                      ConfigMap configMap) {
        if (state == KafkaRebalanceState.Rebalancing) {
            return VertxUtil.completableFutureToVertxFuture(
                            apiClient.getCruiseControlState(reconciliation,
                                    host,
                                    port,
                                    false))
                    .compose(response -> {
                        JsonNode executorState = response.getJson().get("ExecutorState");
                        ExecutorStateProcessor.verifyExecutorState(executorState);
                        updateRebalanceConfigMapWithProgressFields(state, executorState, configMap);
                        return Future.succeededFuture(configMap);
                    });
        } else {
            updateRebalanceConfigMapWithProgressFields(state, null, configMap);
            return Future.succeededFuture(configMap);
        }
    }
}