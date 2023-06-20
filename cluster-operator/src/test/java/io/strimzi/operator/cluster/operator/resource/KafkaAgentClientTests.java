/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;


import org.junit.Test;

import io.strimzi.operator.common.Reconciliation;


public class KafkaAgentClientTests {

    @Test
    public void testBrokerInRecoveryState() throws Exception {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(new Reconciliation("test", "kafka", "namespace", "my-cluster"), "mycluster", "namespace", null, null));
        doAnswer(invocation -> {
            return "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}";
        }).when(kafkaAgentClient).doGet(any());
        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertTrue("broker is not recovery as expected", actual.isBrokerInRecovery());
        assertEquals(10, actual.remainingLogsToRecover());
        assertEquals(100, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testBrokerInRunningState() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(new Reconciliation("test", "kafka", "namespace", "my-cluster"), null, "mycluster", null, null));
        doAnswer(invocation -> {
            return "{\"brokerState\":3}";
        }).when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(3, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testInvalidJsonResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(new Reconciliation("test", "kafka", "namespace", "my-cluster"), null, "mycluster", null, null));
        doAnswer(invocation -> {
            return "&\"brokerState\":3&";
        }).when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testErrorResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(new Reconciliation("test", "kafka", "namespace", "my-cluster"), null, "mycluster", null, null));
        doAnswer(invocation -> {
            throw new RuntimeException("Test failure");
        }).when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }
}
