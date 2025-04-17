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

    private static final String FINISHED_DATA_MOVEMENT_KEY = "finishedDataMovement";
    private static final String TOTAL_DATA_TO_MOVE_KEY = "totalDataToMove";
    private static final String TRIGGERED_TASK_REASON_KEY = "triggeredTaskReason";

    /* test */ enum ExecutorState {
        NO_TASK_IN_PROGRESS,
        STARTING_EXECUTION,
        INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
        INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
        LEADER_MOVEMENT_TASK_IN_PROGRESS,
        STOPPING_EXECUTION,
        INITIALIZING_PROPOSAL_EXECUTION,
        GENERATING_PROPOSALS_FOR_EXECUTION;

        private static final List<ExecutorState> REBALANCE_EXECUTOR_STATES = List.of(
                INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                LEADER_MOVEMENT_TASK_IN_PROGRESS);

        private static ExecutorState fromString(String state) {
            return Arrays.stream(ExecutorState.values())
                    .filter(e -> e.name().equals(state))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown ExecutorState: " + state));
        }
    }

    /**
     * Verifies whether the given executor state is a valid rebalancing state.
     * If the state is not an active rebalancing state, an {@link IllegalStateException} is thrown.
     *
     * @param executorState the {@link JsonNode} containing the executor state to verify;
     *                      must have a {@code "state"} field with a valid executor state string.
     * @throws IllegalStateException if the provided state is not a valid active rebalancing state.
     */
    public static void verifyExecutorState(JsonNode executorState) {
        ExecutorState state = ExecutorState.fromString(executorState.get("state").asText());
        if (!ExecutorState.REBALANCE_EXECUTOR_STATES.contains(state)) {
            throw new IllegalStateException(
                    String.format("Executor has not started rebalance and is currently in non-active state: '%s'. " +
                                  "Progress estimation fields cannot be provided.",
                    state.toString()));
        }
    }

    /**
     * Retrieves the total amount of data to move from the provided `executorState` JSON object.
     * The value is obtained from the "totalDataToMove" field.
     *
     * @param executorState The `JsonNode` object containing the state of the executor,
     *                      from which the total data to move is extracted.
     * @return The total data to move, in megabytes.
     * @throws NullPointerException if the "totalDataToMove" field is missing or null.
     */
    public static Integer getTotalDataToMove(JsonNode executorState) {
        if (!executorState.has(TOTAL_DATA_TO_MOVE_KEY)) {
            throw new IllegalArgumentException(String.format("Executor State does not contain required '%s' field.", TOTAL_DATA_TO_MOVE_KEY));
        }
        return executorState.get(TOTAL_DATA_TO_MOVE_KEY).asInt();
    }

    /**
     * Retrieves the amount of data that has already been moved from the provided `executorState` JSON object.
     * The value is obtained from the "finishedDataMovement" field.
     *
     * @param executorState The `JsonNode` object containing the state of the executor,
     *                      from which the finished data movement is extracted.
     * @return The amount of data that has already been moved, in megabytes.
     */
    public static Integer getFinishedDataMovement(JsonNode executorState) {
        if (!executorState.has(FINISHED_DATA_MOVEMENT_KEY)) {
            throw new IllegalArgumentException(String.format("Executor State does not contain required '%s' field.", FINISHED_DATA_MOVEMENT_KEY));
        }
        return executorState.get(FINISHED_DATA_MOVEMENT_KEY).asInt();
    }

    /**
     * Extracts the task start time from the provided `executorState` JSON object.
     * The task start time is extracted from the "triggeredTaskReason" field, which contains a
     * timestamp in ISO 8601 format. The timestamp is then parsed into seconds since the Unix epoch.
     *
     * @param executorState The `JsonNode` object containing the state of the executor,
     *                      from which the task start time will be extracted.
     * @return The task start time in seconds since the Unix epoch.
     */
    public static Integer getTaskStartTime(JsonNode executorState) {
        if (!executorState.has(TRIGGERED_TASK_REASON_KEY)) {
            throw new IllegalArgumentException(String.format("Executor State does not contain required '%s' field.", TRIGGERED_TASK_REASON_KEY));
        }
        String triggeredTaskReason = executorState.get(TRIGGERED_TASK_REASON_KEY).asText();
        // Extract the timestamp from the string, assuming it's in ISO 8601 format
        String dateString = extractDateFromTriggeredTaskReason(triggeredTaskReason);
        return (int) parseDateToSeconds(dateString);
    }

    /**
     * Method to parse date-time string in ISO 8601 into time in seconds since Unix epoch.
     *
     * @param dateString Date-time string in ISO 8601 format.
     *
     * @return Seconds since Unix epoch.
     */
    private static long parseDateToSeconds(String dateString) {
        // Validate the date format
        if (dateString == null || dateString.isEmpty()) {
            throw new IllegalArgumentException("Invalid date string.");
        }
        // Use Instant to parse the date-time string in ISO 8601 format
        Instant instant = Instant.parse(dateString);
        return instant.getEpochSecond();  // Convert to seconds
    }

    /**
     * Extracts the ISO 8601 date-time string from a Cruise Control task's reason string.
     * The date-time is expected to be in the format "%s (Client: %s, Date: %s)", where
     * the date follows "Date:" in UTC and is rounded to the second
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

        // Regular expression pattern to match the date-time in ISO 8601 format (UTC, rounded to the second).
        String regex = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(triggeredTaskReason);

        if (matcher.find()) {
            // Extract the date string
            return matcher.group();
        }
        throw new IllegalArgumentException("Date not found in triggered task reason.");
    }
}
