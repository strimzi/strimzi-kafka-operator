/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.junit.jupiter.api.Test;

import io.strimzi.operator.common.Reconciliation;

import java.util.Properties;

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
    public void testInvalidBrokerStateJsonResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "&\"brokerState\":3&").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testBrokerStateErrorResponse() {
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
    public void testNodeConfiguration() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "{\"node.id\":\"0\",\"process.roles\":\"controller\"}").when(kafkaAgentClient).doGet(any());

        Properties actual = kafkaAgentClient.getNodeConfiguration("mypod");
        assertThat(actual.get("node.id"), is("0"));
        assertThat(actual.get("process.roles"), is("controller"));
    }

    @Test
    public void testInvalidNodeConfigurationJsonResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace"));
        doAnswer(invocation -> "&\"node.id\":1&").when(kafkaAgentClient).doGet(any());

        Properties actual = kafkaAgentClient.getNodeConfiguration("mypod");
        assertThat(actual, is(nullValue()));
    }
}
