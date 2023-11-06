/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class KafkaAgentClientTest {

    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "kafka", "namespace", "my-cluster");

    @Test
    public void testBrokerInRecoveryState() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}").when(kafkaAgentClient).doGet(any());
        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertTrue(actual.isBrokerInRecovery(), "broker is not in log recovery as expected");
        assertEquals(10, actual.remainingLogsToRecover());
        assertEquals(100, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testBrokerInRunningState() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "{\"brokerState\":3}").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(3, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testInvalidJsonResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "&\"brokerState\":3&").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testErrorResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> {
            throw new RuntimeException("Test failure");
        }).when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testZkMigrationDone() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "{\"state\":1}").when(kafkaAgentClient).doGet(any());

        KRaftMigrationState actual = kafkaAgentClient.getKRaftMigrationState("mypod");
        assertEquals(true, actual.isMigrationDone());
        assertEquals(1, actual.state());
    }

    @Test
    public void testZkMigrationRunning() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "{\"state\":2}").when(kafkaAgentClient).doGet(any());

        KRaftMigrationState actual = kafkaAgentClient.getKRaftMigrationState("mypod");
        assertEquals(false, actual.isMigrationDone());
        assertEquals(2, actual.state());
    }
}
