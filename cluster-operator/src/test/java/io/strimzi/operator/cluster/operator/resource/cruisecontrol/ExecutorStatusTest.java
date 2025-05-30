/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.strimzi.operator.cluster.model.cruisecontrol.CruiseControlExecutorState;
import org.junit.jupiter.api.Test;

import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatus.FINISHED_DATA_MOVEMENT_KEY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatus.STATE_KEY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatus.TOTAL_DATA_TO_MOVE_KEY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatus.TRIGGERED_TASK_REASON_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecutorStatusTest {
    private static final CruiseControlExecutorState DEFAULT_STATE = CruiseControlExecutorState.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
    private static final String DEFAULT_FINISHED_DATA_MOVEMENT = "50";
    private static final String DEFAULT_TOTAL_DATA_TO_MOVE = "1000";
    private static final String DEFAULT_TRIGGERED_TASK_REASON = "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T19:41:27Z)";

    public static ObjectNode createExecutorStatusJson(CruiseControlExecutorState state, String finishedDataMovement, String totalDataToMove, String triggeredTaskReason) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        if (state != null) {
            objectNode.put(STATE_KEY, state.toString());
        }
        if (finishedDataMovement != null) {
            objectNode.put(FINISHED_DATA_MOVEMENT_KEY, finishedDataMovement);
        }
        if (totalDataToMove != null) {
            objectNode.put(TOTAL_DATA_TO_MOVE_KEY, totalDataToMove);
        }
        if (triggeredTaskReason != null) {
            objectNode.put(TRIGGERED_TASK_REASON_KEY, triggeredTaskReason);
        }
        return objectNode;
    }

    @Test
    public void testIsInProgressState() {
        ExecutorStatus es0 = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, null, null, null));
        assertThat(es0.isInProgressState(), is(false));

        ExecutorStatus es1 = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE, DEFAULT_TRIGGERED_TASK_REASON));
        assertThat(es1.isInProgressState(), is(true));
    }

    @Test
    public void testGetFinishedDataMovement() {
        ExecutorStatus es0 = new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, "50", DEFAULT_TOTAL_DATA_TO_MOVE, DEFAULT_TRIGGERED_TASK_REASON));
        assertThat(es0.getFinishedDataMovement(), is(50));

        // Test missing field value is zero
        ExecutorStatus es1 = new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, "", DEFAULT_TOTAL_DATA_TO_MOVE, DEFAULT_TRIGGERED_TASK_REASON));
        assertThat(es1.getFinishedDataMovement(), is(0));

        // Test missing field throws NoSuchFieldException
        assertThrows(IllegalArgumentException.class, () ->
                new ExecutorStatus(createExecutorStatusJson(DEFAULT_STATE, null, null, null)));
    }

    @Test
    public void testGetTotalDataToMove() {
        ExecutorStatus es0 = new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, "10000", DEFAULT_TRIGGERED_TASK_REASON));
        assertThat(es0.getTotalDataToMove(), is(10000));

        // Test missing field value is zero
        ExecutorStatus es1 = new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, "", DEFAULT_TRIGGERED_TASK_REASON));
        assertThat(es1.getTotalDataToMove(), is(0));

        // Test missing field throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () ->
                new ExecutorStatus(createExecutorStatusJson(DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, null, DEFAULT_TRIGGERED_TASK_REASON)));
    }

    @Test
    public void testGetTaskStartTime() {
        ExecutorStatus es0 = new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T19:41:27Z)"));
        assertThat(es0.getTaskStartTime().toString(), is("2024-11-15T19:41:27Z"));

        ExecutorStatus es1 = new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "(Client: 172.17.0.1, Date: 2024-11-10T23:25:27Z)"));
        assertThat(es1.getTaskStartTime().toString(), is("2024-11-10T23:25:27Z"));

        // Test missing date-string fails
        assertThrows(IllegalArgumentException.class, () -> new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE, "")));

        // Test date-string in a non-UTC timezone fails
        assertThrows(IllegalArgumentException.class, () -> new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T20:41:27+01:00)")));

        // Test malformed date-string fails
        assertThrows(IllegalArgumentException.class, () -> new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T20:41:2")));

        // Test missing date-string fails
        assertThrows(IllegalArgumentException.class, () -> new ExecutorStatus(createExecutorStatusJson(
                DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1)")));

        // Test missing field throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () ->
                new ExecutorStatus(createExecutorStatusJson(DEFAULT_STATE, DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE, null)));
    }
}
