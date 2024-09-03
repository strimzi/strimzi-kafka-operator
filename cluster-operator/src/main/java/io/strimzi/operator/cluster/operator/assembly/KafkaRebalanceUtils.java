/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;

import java.util.Arrays;
import java.util.List;

/**
 * Utility class for handling KafkaRebalance custom resource
 */
public class KafkaRebalanceUtils {

    /**
     * Searches through the conditions in the supplied status instance and finds those whose type matches one of the values defined
     * in the {@link KafkaRebalanceState} enum.
     * If there are none it will return null.
     * If there is only one it will return that Condition.
     * If there are more than one it will throw a RuntimeException.
     *
     * @param status The KafkaRebalanceStatus instance whose conditions will be searched.
     * @return The Condition instance from the supplied status that has a type value matching one of the values of the
     *         {@link KafkaRebalanceState} enum. If none are found then the method will return null.
     * @throws RuntimeException If there is more than one Condition instance in the supplied status whose type matches one of the
     *                          {@link KafkaRebalanceState} enum values.
     */
    public static Condition rebalanceStateCondition(KafkaRebalanceStatus status) {
        if (status.getConditions() != null) {

            List<Condition> statusConditions = status.getConditions()
                    .stream()
                    .filter(condition -> condition.getType() != null)
                    .filter(condition -> Arrays.stream(KafkaRebalanceState.values())
                            .anyMatch(stateValue -> stateValue.toString().equals(condition.getType())))
                    .toList();

            if (statusConditions.size() == 1) {
                return statusConditions.get(0);
            } else if (statusConditions.size() > 1) {
                throw new RuntimeException("Multiple KafkaRebalance State Conditions were present in the KafkaRebalance status");
            }
        }
        // If there are no conditions or none that have the correct status
        return null;
    }

    /**
     * Extract the {@link KafkaRebalanceState} enum state from the status of the corresponding KafkaRebalance custom resource
     *
     * @param kafkaRebalanceStatus  KafkaRebalance custom resource status from which extracting the rebalancing state
     * @return  the corresponding {@link KafkaRebalanceState} enum state
     */
    public static KafkaRebalanceState rebalanceState(KafkaRebalanceStatus kafkaRebalanceStatus) {
        if (kafkaRebalanceStatus != null) {
            Condition rebalanceStateCondition = rebalanceStateCondition(kafkaRebalanceStatus);
            String statusString = rebalanceStateCondition != null ? rebalanceStateCondition.getType() : null;
            if (statusString != null) {
                return KafkaRebalanceState.valueOf(statusString);
            }
        }
        return null;
    }

    /**
     * Compose the name of the KafkaRebalance custom resource to be used for running an auto-rebalancing in the specified mode
     * for the specified Kafka cluster
     *
     * @param cluster   Kafka cluster name (from Kafka custom resource metadata)
     * @param kafkaRebalanceMode    Auto-rebalance mode
     * @return  the name of the KafkaRebalance custom resource to be used for running an auto-rebalancing
     *          in the specified mode for the specified Kafka cluster
     */
    public static String autoRebalancingKafkaRebalanceResourceName(String cluster, KafkaRebalanceMode kafkaRebalanceMode) {
        return cluster + "-auto-rebalancing-" + kafkaRebalanceMode.toValue();
    }
}
