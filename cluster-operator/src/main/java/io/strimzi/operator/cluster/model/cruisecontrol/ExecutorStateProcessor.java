/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for extracting fields from ExecutorState JSON from Cruise Control REST API.
 */
public class ExecutorStateProcessor {

    // Regular expression pattern to match the date-time in ISO 8601 format (UTC, rounded to the second).
    private static final Pattern ISO_8601_UTC_TIMESTAMP_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");

    /* test */ static final String FINISHED_DATA_MOVEMENT_KEY = "finishedDataMovement";
    /* test */ static final String TOTAL_DATA_TO_MOVE_KEY = "totalDataToMove";
    /* test */ static final String TRIGGERED_TASK_REASON_KEY = "triggeredTaskReason";

    /**
     * Represents the state that the Cruise Control Executor can be in at a moment in time.
     */
    public enum ExecutorState {
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

        private static final List<ExecutorState> REBALANCE_EXECUTOR_STATES = List.of(
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

            ExecutorState state = fromString(executorJson.get("state").asText());
            if (!ExecutorState.REBALANCE_EXECUTOR_STATES.contains(state)) {
                throw new IllegalStateException(
                        String.format("Executor has not started rebalance and is currently in non-active state: '%s'. " +
                                        "Progress estimation fields cannot be provided.",
                                state.toString()));
            }
        }

        private static ExecutorState fromString(String state) {
            String normalized = state.trim();
            return Arrays.stream(ExecutorState.values())
                    .filter(e -> e.name().equals(normalized))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown ExecutorState: " + state));
        }
    }

    /**
     * Retrieves the total amount of data to move from the provided `executorJson` JSON object.
     * The value is obtained from the "totalDataToMove" field.
     *
     * @param executorJson The `JsonNode` object containing the state of the executor,
     *                      from which the total data to move is extracted.
     * @return The total data to move, in megabytes.
     * @throws NullPointerException if the "totalDataToMove" field is missing or null.
     */
    public static Integer getTotalDataToMove(JsonNode executorJson) {
        if (!executorJson.has(TOTAL_DATA_TO_MOVE_KEY)) {
            throw new IllegalArgumentException(String.format("Executor State does not contain required '%s' field.", TOTAL_DATA_TO_MOVE_KEY));
        }
        return executorJson.get(TOTAL_DATA_TO_MOVE_KEY).asInt();
    }

    /**
     * Retrieves the amount of data that has already been moved from the provided `executorJson` JSON object.
     * The value is obtained from the "finishedDataMovement" field.
     *
     * @param executorJson The `JsonNode` object containing the state of the executor,
     *                      from which the finished data movement is extracted.
     * @return The amount of data that has already been moved, in megabytes.
     */
    public static Integer getFinishedDataMovement(JsonNode executorJson) {
        if (!executorJson.has(FINISHED_DATA_MOVEMENT_KEY)) {
            throw new IllegalArgumentException(String.format("Executor State does not contain required '%s' field.", FINISHED_DATA_MOVEMENT_KEY));
        }
        return executorJson.get(FINISHED_DATA_MOVEMENT_KEY).asInt();
    }

    /**
     * Extracts the task start time from the provided `executorJson` JSON object.
     * The task start time is extracted from the "triggeredTaskReason" field, which contains a
     * timestamp in ISO 8601 format. The timestamp is then parsed into Instant object.
     *
     * Update this method to extract the task start time from `StartMs` field once this issue is resolved:
     * https://github.com/linkedin/cruise-control/issues/2271
     *
     * @param executorJson The `JsonNode` object containing the state of the executor,
     *                      from which the task start time will be extracted.
     * @return The task start time as an Instant object.
     */
    public static Instant getTaskStartTime(JsonNode executorJson) {
        if (!executorJson.has(TRIGGERED_TASK_REASON_KEY)) {
            throw new IllegalArgumentException(String.format("Executor State does not contain required '%s' field.", TRIGGERED_TASK_REASON_KEY));
        }
        String triggeredTaskReason = executorJson.get(TRIGGERED_TASK_REASON_KEY).asText();
        // Extract the timestamp from the string, assuming it's in ISO 8601 format
        String dateString = extractDateFromTriggeredTaskReason(triggeredTaskReason);

        // Validate the date format
        if (dateString == null || dateString.isEmpty()) {
            throw new IllegalArgumentException("Date string is null or empty.");
        }

        return Instant.parse(dateString);
    }

    /**
     * Extracts the ISO 8601 date-time string from a Cruise Control task's triggeredTaskReason string.
     * The triggeredTaskReason string is expected to be in the format "%s (Client: %s, Date: %s)", where
     * the ISO 8601 date-time string follows "Date:" in UTC and is rounded to the second
     * (see: https://github.com/linkedin/cruise-control/blob/main/cruise-control-core/src/main/java/com/linkedin/cruisecontrol/CruiseControlUtils.java#L39-L41).
     *
     * @param triggeredTaskReason Cruise Control task's triggeredTaskReason string.
     *
     * @return Date-time string in ISO 8601 format.
     */
    private static String extractDateFromTriggeredTaskReason(String triggeredTaskReason) {
        if (triggeredTaskReason == null || triggeredTaskReason.isEmpty()) {
            throw new IllegalArgumentException("Triggered task reason is missing.");
        }

        Matcher matcher = ISO_8601_UTC_TIMESTAMP_PATTERN.matcher(triggeredTaskReason);
        if (matcher.find()) {
            // Extract the date string
            return matcher.group();
        }
        throw new IllegalArgumentException("Date not found in triggered task reason.");
    }
}
