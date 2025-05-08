/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

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

    private static final List<CruiseControlExecutorState> PROGRESS_STATES = List.of(
            INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
            INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
            LEADER_MOVEMENT_TASK_IN_PROGRESS);

    /**
     * Determines if given executor state is in a valid progress state.
     *
     * @param state the {@link CruiseControlExecutorState} containing the executor state to verify;
     * @return True if given executor state is in valid progress state.
     *         If the state is not an active progress state return false.
     */
    public static boolean inProgressState(CruiseControlExecutorState state) {
        return PROGRESS_STATES.contains(state);
    }

    /**
     * Converts string to CruiseControlExecutorState object
     *
     * @param state The executor status state as a string.
     * @return The executor status state as a CruiseControlExecutorState object.
     */
    public static CruiseControlExecutorState fromString(String state) {
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
