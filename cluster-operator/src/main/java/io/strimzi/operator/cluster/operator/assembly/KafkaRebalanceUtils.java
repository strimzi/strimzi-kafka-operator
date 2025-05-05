/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokers;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokersBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.common.model.StatusUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
     * Starting from the provided KafkaAutoRebalanceStatus instance which brings the current auto-rebalance modes and brokers for
     * ongoing auto-rebalancing and from the set of brokers currently added by a scaling up operation, this method updates the KafkaStatus
     * instance with a consistent list of added brokers (current and new) into the corresponding auto-rebalance part.
     *
     * @param kafkaStatus   the KafkaStatus instance to be updated with list of added brokers (current and new) withing the auto-rebalance part
     * @param kafkaAutoRebalanceStatus  the current auto-rebalance status with modes and brokers for ongoing auto-rebalancing operations
     * @param kafkaAddedBrokerNodes the set of newly added brokers to the cluster during the current reconciliation
     */
    public static void updateKafkaAutoRebalanceStatus(KafkaStatus kafkaStatus, KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus, Set<Integer> kafkaAddedBrokerNodes) {
        // we are going to save a consistent list of added notes (between the ones already in the status and the newly added ones)
        // into the KafkaStatus autorebalance field to be passed to the auto-rebalancing reconciler.
        // It's needed in case nodes are scaled up but reconciliation fails and on the next one the operator has to know where to rebalance

        Set<Integer> addedBrokers = new HashSet<>(kafkaAddedBrokerNodes);
        if (kafkaAutoRebalanceStatus == null) {
            KafkaAutoRebalanceStatusBuilder kafkaAutoRebalanceStatusBuilder = new KafkaAutoRebalanceStatusBuilder()
                    .withLastTransitionTime(StatusUtils.iso8601Now())
                    .withState(KafkaAutoRebalanceState.Idle);
            // no already stored added nodes, adding only the new ones from current reconciliation
            if (!addedBrokers.isEmpty()) {
                kafkaAutoRebalanceStatusBuilder.withModes(
                        new KafkaAutoRebalanceStatusBrokersBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withBrokers(kafkaAddedBrokerNodes.stream().toList())
                                .build()
                );
            }
            kafkaStatus.setAutoRebalance(kafkaAutoRebalanceStatusBuilder.build());
        } else {
            KafkaAutoRebalanceStatusBuilder builder = new KafkaAutoRebalanceStatusBuilder(kafkaAutoRebalanceStatus);
            if (kafkaAutoRebalanceStatus.getModes() == null) {
                // no already stored added nodes, adding only the new ones from current reconciliation
                if (!addedBrokers.isEmpty()) {
                    builder.withModes(
                            new KafkaAutoRebalanceStatusBrokersBuilder()
                                    .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                    .withBrokers(kafkaAddedBrokerNodes.stream().toList())
                                    .build()
                    );
                }
                kafkaStatus.setAutoRebalance(builder.build());
            } else {
                // going to create a consistent list between:
                // the added nodes already stored in the status
                // the newly added nodes within the current reconciliation

                // Merge the already stored brokers with new ones if the mode exists, or create a new mode
                Optional<KafkaAutoRebalanceStatusBrokers> existingAddBrokersMode = kafkaAutoRebalanceStatus.getModes().stream()
                        .filter(mode -> mode.getMode().equals(KafkaAutoRebalanceMode.ADD_BROKERS))
                        .findFirst();

                if (existingAddBrokersMode.isPresent()) {
                    addedBrokers.addAll(existingAddBrokersMode.get().getBrokers());
                    builder.editMatchingMode(m -> m.getMode().equals(KafkaAutoRebalanceMode.ADD_BROKERS))
                            .withBrokers(addedBrokers.stream().toList())
                            .endMode();
                } else if (!addedBrokers.isEmpty()) {
                    builder.addNewMode()
                            .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                            .withBrokers(addedBrokers.stream().toList())
                            .endMode();
                }
                kafkaStatus.setAutoRebalance(builder.build());
            }
        }
    }

    /**
     * Updates the {@code KafkaRebalanceStatus} with a new Warning condition based
     * on the provided {@link Throwable}. If a Warning condition with the same reason
     * and message already exists, no update is performed.
     *
     * @param status    The {@link KafkaRebalanceStatus} object whose conditions will be updated.
     * @param exception The {@link Throwable} containing the reason and message for the Warning condition.
     */
    public static void addWarningCondition(KafkaRebalanceStatus status, Throwable exception) {
        List<Condition> conditions = status.getConditions() != null ? new ArrayList<>(status.getConditions()) : new ArrayList<>();

        Condition newCondition = StatusUtils.buildWarningCondition("CruiseControlExecutorState", exception.getMessage());
        Condition oldCondition = getWarningCondition(status);

        if (oldCondition != null) {
            // If existing Warning condition has same reason & message, do nothing
            if (Objects.equals(newCondition.getReason(), oldCondition.getReason()) &&
                Objects.equals(newCondition.getMessage(), oldCondition.getMessage())) {
                return;
            }
            // Otherwise, remove existing Warning condition
            conditions.remove(oldCondition);
        }

        // Add new Warning condition
        conditions.add(newCondition);
        status.setConditions(conditions);
    }

    /* test */ static Condition getWarningCondition(KafkaRebalanceStatus status) {
        return status.getConditions().stream()
                .filter(condition -> condition.getType().equals("Warning"))
                .findFirst()
                .orElse(null);
    }
}
