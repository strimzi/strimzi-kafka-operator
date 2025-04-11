/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.ExecutorStateProcessor;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for updating data in KafkaRebalance ConfigMap
 */
public class KafkaRebalanceConfigMapUtils {

    /* test */ static final String ESTIMATED_TIME_TO_COMPLETION_KEY = "estimatedTimeToCompletionInMinutes";
    /* test */ static final String COMPLETED_BYTE_MOVEMENT_KEY = "completedByteMovementPercentage";
    /* test */ static final String EXECUTOR_STATE_KEY = "executorState.json";
    /* test */ static final String BROKER_LOAD_KEY = "brokerLoad.json";

    /* test */ static final String TIME_COMPLETED = "0";
    private static final String BYTE_MOVEMENT_ZERO = "0";
    /* test */ static final String BYTE_MOVEMENT_COMPLETED = "100";

    private static ConfigMap createConfigMap(KafkaRebalance kafkaRebalance) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withNamespace(kafkaRebalance.getMetadata().getNamespace())
                .withName(kafkaRebalance.getMetadata().getName())
                .withLabels(Collections.singletonMap("app", "strimzi"))
                .withOwnerReferences(ModelUtils.createOwnerReference(kafkaRebalance, false))
                .endMetadata()
                .build();
    }

    /**
     * Updates the given KafkaRebalance ConfigMap with the broker load field.
     *
     * @param beforeAndAfterBrokerLoad The broker load information in JSON format, which is used to populate the broker load field.
     * @param configMap The ConfigMap to be updated with progress information.
     */
    public static void updateRebalanceConfigMapWithLoadField(JsonNode beforeAndAfterBrokerLoad, ConfigMap configMap) {
        Map<String, String> map = configMap.getData();
        map.put(BROKER_LOAD_KEY, beforeAndAfterBrokerLoad.toString());
    }

    /**
     * Updates the given KafkaRebalance ConfigMap with progress fields based on the progress of the Kafka rebalance operation.
     *
     * @param state The current state of the Kafka rebalance operation (e.g., ProposalReady, Rebalancing, Stopped, etc.).
     * @param executorState The executor state information in JSON format, which is used to calculate progress fields.
     * @param configMap The ConfigMap to be updated with progress information. If null, a new ConfigMap will be created.
     */
    public static void updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState state, JsonNode executorState, ConfigMap configMap) {
        if (configMap == null) {
            configMap = new ConfigMap();
        }

        Map<String, String> data = configMap.getData();
        if (data == null) {
            data = new HashMap<>();
            configMap.setData(data);
        }

        switch (state) {
            case ProposalReady:
                data.remove(ESTIMATED_TIME_TO_COMPLETION_KEY);
                data.put(COMPLETED_BYTE_MOVEMENT_KEY, BYTE_MOVEMENT_ZERO);
                data.remove(EXECUTOR_STATE_KEY);
                break;
            case Rebalancing:
                try {
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
                } catch (IllegalArgumentException | ArithmeticException e) {
                    // TODO: Have this caught by caller, logged and put into the `KafkaRebalance` status
                    throw new RuntimeException(e);
                }
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

    static Future<ConfigMap> updateRebalanceConfigMap(Reconciliation reconciliation,
                                                      ConfigMapOperator configMapOperator,
                                                      KafkaRebalance kafkaRebalance,
                                                      KafkaRebalanceState state,
                                                      JsonNode executorState,
                                                      JsonNode beforeAndAfterBrokerLoad) {

        Future<ConfigMap> oldKafkaRebalanceConfigMap;
        Future<ConfigMap> newKafkaRebalanceConfigMap;

        ConfigMap kafkaRebalanceConfigMap = createConfigMap(kafkaRebalance);
        String namespace = kafkaRebalance.getMetadata().getNamespace();
        String name = kafkaRebalance.getMetadata().getName();

        switch (state) {
            case New:
            case PendingProposal:
                newKafkaRebalanceConfigMap = Future.succeededFuture(null);
                break;
            case ProposalReady:
                updateRebalanceConfigMapWithLoadField(beforeAndAfterBrokerLoad, kafkaRebalanceConfigMap);
                updateRebalanceConfigMapWithProgressFields(state, null, kafkaRebalanceConfigMap);
                newKafkaRebalanceConfigMap = Future.succeededFuture(kafkaRebalanceConfigMap);
                break;
            case Rebalancing:
                oldKafkaRebalanceConfigMap = configMapOperator.getAsync(namespace, name);
                newKafkaRebalanceConfigMap = oldKafkaRebalanceConfigMap.flatMap(configMap -> {
                    updateRebalanceConfigMapWithProgressFields(state, executorState, configMap);
                    return Future.succeededFuture(configMap);
                });
                break;
            case Stopped:
            case Ready:
            case NotReady:
                oldKafkaRebalanceConfigMap = configMapOperator.getAsync(namespace, name);
                newKafkaRebalanceConfigMap = oldKafkaRebalanceConfigMap.flatMap(configMap -> {
                    updateRebalanceConfigMapWithProgressFields(state, null, configMap);
                    return Future.succeededFuture(configMap);
                });
                break;
            case ReconciliationPaused:
                newKafkaRebalanceConfigMap = Future.succeededFuture();
                break;

            default:
                newKafkaRebalanceConfigMap = Future.succeededFuture(null);
                break;
        }

        return newKafkaRebalanceConfigMap;
    }

    static Future<ReconcileResult<ConfigMap>> reconcileKafkaRebalanceConfigMap(Reconciliation reconciliation,
                                                                               ConfigMapOperator configMapOperator,
                                                                               KafkaRebalance kafkaRebalance,
                                                                               KafkaRebalanceState state,
                                                                               JsonNode executorState,
                                                                               JsonNode beforeAndAfterBrokerLoad) {
        String namespace = kafkaRebalance.getMetadata().getNamespace();
        String name = kafkaRebalance.getMetadata().getName();
        Future<ConfigMap> configMapFuture = updateRebalanceConfigMap(reconciliation, configMapOperator, kafkaRebalance, state, executorState, beforeAndAfterBrokerLoad);
        return configMapFuture.flatMap(configMap -> {
            return configMapOperator.reconcile(reconciliation, namespace, name, configMap);
        });
    }

}