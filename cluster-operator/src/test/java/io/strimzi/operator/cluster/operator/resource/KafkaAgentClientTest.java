/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.sun.net.httpserver.HttpServer;
import io.strimzi.operator.cluster.auth.RequestedServiceAccountAuthIdentity;
import io.strimzi.operator.cluster.auth.ServiceAccountToken;
import io.strimzi.operator.cluster.auth.ServiceAccountTokenService;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.Identity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class KafkaAgentClientTest {
    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "kafka", "namespace", "my-cluster");
    private static final String AUDIENCE = "strimzi.io/kafka/namespace/my-cluster";
    private static final long EXPIRATION_SECONDS = 3600L;

    private HttpServer httpServer;
    private String receivedAuthorization;

    @AfterEach
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    private static RequestedServiceAccountAuthIdentity authIdentity() {
        return new RequestedServiceAccountAuthIdentity(RECONCILIATION, AUDIENCE, EXPIRATION_SECONDS);
    }

    /**
     * Starts a plain HTTP server which records the Authorization header of the received request and returns the
     * broker state JSON.
     *
     * @return  URI of the endpoint served by the server
     */
    private URI startHttpServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        httpServer.createContext("/v1/broker-state/", exchange -> {
            receivedAuthorization = exchange.getRequestHeaders().getFirst("Authorization");

            byte[] body = "{\"brokerState\":3}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);

            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        httpServer.start();

        return URI.create("http://localhost:" + httpServer.getAddress().getPort() + "/v1/broker-state/");
    }

    @Test
    public void testBrokerInRecoveryState() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null)));
        doAnswer(invocation -> "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}").when(kafkaAgentClient).doGet(any());
        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertTrue(actual.isBrokerInRecovery(), "broker is not in log recovery as expected");
        assertEquals(10, actual.remainingLogsToRecover());
        assertEquals(100, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testBrokerInRunningState() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null)));
        doAnswer(invocation -> "{\"brokerState\":3}").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(3, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testInvalidJsonResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null)));
        doAnswer(invocation -> "&\"brokerState\":3&").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testErrorResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null)));
        doAnswer(invocation -> {
            throw new RuntimeException("Test failure");
        }).when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testRequestIsSentWithTheServiceAccountToken() throws IOException {
        // The token service is mocked here. Its own caching and renewal is covered by the ServiceAccountTokenServiceTest.
        ServiceAccountTokenService tokenService = mock(ServiceAccountTokenService.class);
        when(tokenService.token(any(RequestedServiceAccountAuthIdentity.class))).thenReturn(new ServiceAccountToken("my-token", System.currentTimeMillis(), System.currentTimeMillis() + 1000 * EXPIRATION_SECONDS));

        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity())));
        doReturn(tokenService).when(kafkaAgentClient).tokenService();

        assertThat(kafkaAgentClient.doGet(startHttpServer()), is("{\"brokerState\":3}"));
        assertThat(receivedAuthorization, is("Bearer my-token"));
    }

    @Test
    public void testRequestIsSentWithoutTokenWhenServiceAccountAuthenticationIsNotUsed() throws IOException {
        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null));

        assertThat(kafkaAgentClient.doGet(startHttpServer()), is("{\"brokerState\":3}"));
        assertThat(receivedAuthorization, is(nullValue()));
    }
}
