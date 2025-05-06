/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Represents the state that the Cruise Control Executor can be in at a moment in time.
 */
public enum CruiseControlExecutorState {
    /**
     * Indicates that there is no task currently in progress.
     */
    NO_TASK_IN_PROGRESS,
    /**
     * Indicates that there is task is being prepared for execution.
     */
    STARTING_EXECUTION,
    /**
     * Indicates that there is an inter-broker partition movement task in progress.
     */
    INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    /**
     * Indicates that there is an intra-broker partition movement task in progress.
     */
    INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    /**
     * Indicates that there is leadership partition movement task in progress.
     */
    LEADER_MOVEMENT_TASK_IN_PROGRESS,
    /**
     * Indicates the executor is in the process of stopping task execution.
     */
    STOPPING_EXECUTION,
    /**
     * Indicates the executor is preparing a task for execution.
     */
    INITIALIZING_PROPOSAL_EXECUTION,
    /**
     * Indicates the executor is generating partition movement proposals for a task for execution.
     */
    GENERATING_PROPOSALS_FOR_EXECUTION;

    private static final List<CruiseControlExecutorState> REBALANCE_EXECUTOR_STATES = List.of(
            INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
            INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
            LEADER_MOVEMENT_TASK_IN_PROGRESS);

    /**
     * Verifies whether the given executor state is a valid rebalancing state.
     * If the state is not an active rebalancing state, an {@link IllegalStateException} is thrown.
     *
     * @param executorJson the {@link JsonNode} containing the executor state to verify;
     *                      must have a {@code "state"} field with a valid executor state string.
     * @throws IllegalStateException if the provided state is not a valid active rebalancing state.
     */
    public static void verifyRebalancingState(JsonNode executorJson) {
        if (executorJson == null || !executorJson.has("state")) {
            throw new IllegalStateException(
                    String.format("Executor state: `%s` does not contain \"state\" entry", executorJson));
        }

        CruiseControlExecutorState state = fromString(executorJson.get("state").asText());
        if (!REBALANCE_EXECUTOR_STATES.contains(state)) {
            throw new IllegalStateException(
                    String.format("Executor has not started rebalance and is currently in non-active state: '%s'. " +
                                    "Progress estimation fields cannot be provided.",
                            state.toString()));
        }
    }

    private static CruiseControlExecutorState fromString(String state) {
        if (state == null) {
            throw new IllegalArgumentException("ExecutorState cannot be null");
        }

        try {
            return valueOf(state);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown ExecutorState: " + state, e);
        }
    }
}
