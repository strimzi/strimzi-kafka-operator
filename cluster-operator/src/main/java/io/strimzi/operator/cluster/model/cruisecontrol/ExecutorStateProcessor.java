/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
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
