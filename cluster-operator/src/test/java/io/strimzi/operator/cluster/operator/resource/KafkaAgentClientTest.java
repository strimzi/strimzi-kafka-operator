/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.strimzi.operator.cluster.auth.RequestedServiceAccountAuthIdentity;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.Identity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    private static TokenRequest tokenRequestResponse(String token, Instant expiration) {
        return new TokenRequestBuilder()
                .withNewStatus()
                    .withToken(token)
                    .withExpirationTimestamp(expiration != null ? expiration.toString() : null)
                .endStatus()
                .build();
    }

    @SuppressWarnings("unchecked")
    private static KubernetesClient mockKubernetesClient(ServiceAccountResource serviceAccountResource) {
        NonNamespaceOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> namespacedOp = mock(NonNamespaceOperation.class);
        when(namespacedOp.withName("my-cluster-cluster-operator")).thenReturn(serviceAccountResource);

        MixedOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> op = mock(MixedOperation.class);
        when(op.inNamespace("namespace")).thenReturn(namespacedOp);

        KubernetesClient client = mock(KubernetesClient.class);
        when(client.serviceAccounts()).thenReturn(op);

        return client;
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
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null), null));
        doAnswer(invocation -> "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}").when(kafkaAgentClient).doGet(any());
        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertTrue(actual.isBrokerInRecovery(), "broker is not in log recovery as expected");
        assertEquals(10, actual.remainingLogsToRecover());
        assertEquals(100, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testBrokerInRunningState() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null), null));
        doAnswer(invocation -> "{\"brokerState\":3}").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(3, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testInvalidJsonResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null), null));
        doAnswer(invocation -> "&\"brokerState\":3&").when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testErrorResponse() {
        KafkaAgentClient kafkaAgentClient = spy(new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null), null));
        doAnswer(invocation -> {
            throw new RuntimeException("Test failure");
        }).when(kafkaAgentClient).doGet(any());

        BrokerState actual = kafkaAgentClient.getBrokerState("mypod");
        assertEquals(-1, actual.code());
        assertEquals(0, actual.remainingLogsToRecover());
        assertEquals(0, actual.remainingSegmentsToRecover());
    }

    @Test
    public void testTokenIsRequestedForTheClusterOperatorServiceAccount() {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-token"));

        ArgumentCaptor<TokenRequest> requestCaptor = ArgumentCaptor.forClass(TokenRequest.class);
        verify(serviceAccountResource).tokenRequest(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getSpec().getAudiences(), is(List.of(AUDIENCE)));
        assertThat(requestCaptor.getValue().getSpec().getExpirationSeconds(), is(EXPIRATION_SECONDS));
    }

    @Test
    public void testTokenIsCachedAndReused() {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-token"));
        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-token"));

        verify(serviceAccountResource, times(1)).tokenRequest(any());
    }

    @Test
    public void testTokenIsCachedWhenKubernetesShortensTheRequestedExpiration() {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        // The Kubernetes API can issue the token with a shorter validity than requested. The renewal has to follow the
        // returned expiration time and not the requested one, otherwise the token would be renewed on every call.
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse("my-token", Instant.now().plusSeconds(60)));

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-token"));
        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-token"));

        verify(serviceAccountResource, times(1)).tokenRequest(any());
    }

    @Test
    public void testTokenIsRenewedWhenItIsDueForRefresh() {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any()))
                // The first token is already past its refresh time and should not be reused
                .thenReturn(tokenRequestResponse("my-old-token", Instant.now().minusSeconds(EXPIRATION_SECONDS)))
                .thenReturn(tokenRequestResponse("my-new-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-old-token"));
        assertThat(kafkaAgentClient.currentToken(authIdentity()), is("my-new-token"));

        verify(serviceAccountResource, times(2)).tokenRequest(any());
    }

    @Test
    public void testFailsWhenKubernetesApiReturnsNoToken() {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse(null, Instant.now().plusSeconds(EXPIRATION_SECONDS)));

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        RuntimeException e = assertThrows(RuntimeException.class, () -> kafkaAgentClient.currentToken(authIdentity()));
        assertThat(e.getMessage(), is("Kubernetes API did not return a token for ServiceAccount namespace/my-cluster-cluster-operator"));
    }

    @Test
    public void testFailsWhenKubernetesApiReturnsNoResponse() {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(null);

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        assertThrows(RuntimeException.class, () -> kafkaAgentClient.currentToken(authIdentity()));
    }

    @Test
    public void testRequestIsSentWithTheServiceAccountToken() throws IOException {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));

        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, authIdentity()), mockKubernetesClient(serviceAccountResource));

        assertThat(kafkaAgentClient.doGet(startHttpServer()), is("{\"brokerState\":3}"));
        assertThat(receivedAuthorization, is("Bearer my-token"));
    }

    @Test
    public void testRequestIsSentWithoutTokenWhenServiceAccountAuthenticationIsNotUsed() throws IOException {
        KafkaAgentClient kafkaAgentClient = new KafkaAgentClient(RECONCILIATION, "my-cluster", "namespace", new Identity(null, null), null);

        assertThat(kafkaAgentClient.doGet(startHttpServer()), is("{\"brokerState\":3}"));
        assertThat(receivedAuthorization, is(nullValue()));
    }
}
