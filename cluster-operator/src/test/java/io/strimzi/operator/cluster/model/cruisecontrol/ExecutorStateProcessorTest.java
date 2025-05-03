/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.strimzi.operator.cluster.model.cruisecontrol.ExecutorStateProcessor.ExecutorState;
import static io.strimzi.operator.cluster.model.cruisecontrol.ExecutorStateProcessor.FINISHED_DATA_MOVEMENT_KEY;
import static io.strimzi.operator.cluster.model.cruisecontrol.ExecutorStateProcessor.TOTAL_DATA_TO_MOVE_KEY;
import static io.strimzi.operator.cluster.model.cruisecontrol.ExecutorStateProcessor.TRIGGERED_TASK_REASON_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecutorStateProcessorTest {
    private static final String DEFAULT_FINISHED_DATA_MOVEMENT = "50";
    private static final String DEFAULT_TOTAL_DATA_TO_MOVE = "1000";
    private static final String DEFAULT_TRIGGERED_TASK_REASON = "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T19:41:27Z)";

    private static final String STATE_KEY = "state";

    private static ObjectNode createExecutorState(Map<String, String> executorState) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        if (executorState != null) {
            for (Map.Entry<String, String> entry : executorState.entrySet()) {
                objectNode.put(entry.getKey(), entry.getValue());
            }
        }
        return objectNode;
    }

    public static ObjectNode createExecutorState(String finishedDataMovement, String totalDataToMove, String triggeredTaskReason) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
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
    public void testVerifyRebalancingState() throws Exception {
        JsonNode es0 = createExecutorState(Map.of(STATE_KEY, ExecutorState.NO_TASK_IN_PROGRESS.toString()));
        assertThrows(IllegalStateException.class, () -> ExecutorState.verifyRebalancingState(es0));

        JsonNode es1 = createExecutorState(Map.of(STATE_KEY, ExecutorState.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS.toString()));
        ExecutorState.verifyRebalancingState(es1);

        JsonNode es2 = createExecutorState(Map.of("", ""));
        assertThrows(IllegalStateException.class, () -> ExecutorState.verifyRebalancingState(es2));
    }

    @Test
    public void testGetFinishedDataMovement() throws Exception {
        JsonNode es0 = createExecutorState("50", DEFAULT_TOTAL_DATA_TO_MOVE, DEFAULT_TRIGGERED_TASK_REASON);
        assertThat(ExecutorStateProcessor.getFinishedDataMovement(es0), is(50));

        // Test missing field value is zero
        JsonNode es1 = createExecutorState("", DEFAULT_TOTAL_DATA_TO_MOVE, DEFAULT_TRIGGERED_TASK_REASON);
        assertThat(ExecutorStateProcessor.getFinishedDataMovement(es1), is(0));

        // Test missing field throws NoSuchFieldException
        JsonNode es2 = createExecutorState(null);
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getFinishedDataMovement(es2));
    }

    @Test
    public void testGetTotalDataToMove() throws Exception {
        JsonNode es0 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, "10000", DEFAULT_TRIGGERED_TASK_REASON);
        assertThat(ExecutorStateProcessor.getTotalDataToMove(es0), is(10000));

        // Test missing field value is zero
        JsonNode es1 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, "", DEFAULT_TRIGGERED_TASK_REASON);
        assertThat(ExecutorStateProcessor.getTotalDataToMove(es1), is(0));

        // Test missing field throws NoSuchFieldException
        JsonNode es2 = createExecutorState(null);
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getTotalDataToMove(es2));
    }

    @Test
    public void testGetTaskStartTime() throws Exception {
        JsonNode es0 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T19:41:27Z)");
        assertThat(ExecutorStateProcessor.getTaskStartTime(es0).toString(), is("2024-11-15T19:41:27Z"));

        JsonNode es1 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "(Client: 172.17.0.1, Date: 2024-11-10T23:25:27Z)");
        assertThat(ExecutorStateProcessor.getTaskStartTime(es1).toString(), is("2024-11-10T23:25:27Z"));

        // Test missing date-string fails
        JsonNode es2 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE, "");
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getTaskStartTime(es2));

        // Test date-string in a non-UTC timezone fails
        JsonNode es3 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T20:41:27+01:00)");
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getTaskStartTime(es3));

        // Test malformed date-string fails
        JsonNode es4 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1, Date: 2024-11-15T20:41:2");
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getTaskStartTime(es4));

        // Test missing date-string fails
        JsonNode es5 = createExecutorState(DEFAULT_FINISHED_DATA_MOVEMENT, DEFAULT_TOTAL_DATA_TO_MOVE,
                "No reason provided (Client: 172.17.0.1)");
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getTaskStartTime(es5));

        // Test missing field throws NoSuchFieldException
        JsonNode es6 = createExecutorState(null);
        assertThrows(IllegalArgumentException.class, () -> ExecutorStateProcessor.getTaskStartTime(es6));
    }
}