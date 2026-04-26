/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;

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

    /**
     * Regression guard: a previous version of KafkaAgentClient configured no timeout on the per-request side,
     * which let a broker stuck on IO block the KafkaRoller's single-threaded executor indefinitely. Verify that
     * requests built via the package-private helper carry the configured timeout.
     */
    @Test
    public void testBuildRequestAppliesHttpRequestTimeout() {
        HttpRequest request = KafkaAgentClient.buildRequest(URI.create("https://example.invalid/v1/broker-state/"));
        Duration timeout = request.timeout().orElseThrow(() -> new AssertionError("HTTP request timeout was not configured"));
        assertEquals(KafkaAgentClient.HTTP_REQUEST_TIMEOUT, timeout, "Request timeout must equal HTTP_REQUEST_TIMEOUT");
        assertTrue(timeout.toMillis() > 0, "HTTP request timeout must be positive but was " + timeout);
    }

    /**
     * Regression guard: a previous version of KafkaAgentClient configured no connect timeout, which let an
     * unresponsive broker block the KafkaRoller's single-threaded executor indefinitely. Verify that clients
     * built via the package-private helper carry the configured connect timeout.
     */
    @Test
    public void testHttpClientBuilderAppliesConnectTimeout() {
        HttpClient client = KafkaAgentClient.httpClientBuilder().build();
        Duration connectTimeout = client.connectTimeout().orElseThrow(() -> new AssertionError("HTTP connect timeout was not configured"));
        assertEquals(KafkaAgentClient.HTTP_REQUEST_TIMEOUT, connectTimeout, "Connect timeout must equal HTTP_REQUEST_TIMEOUT");
        assertTrue(connectTimeout.toMillis() > 0, "HTTP connect timeout must be positive but was " + connectTimeout);
    }
}
