/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountTokenServiceTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String SERVICE_ACCOUNT = "my-cluster-cluster-operator";
    private static final String AUDIENCE = "strimzi.io/kafka/my-namespace/my-cluster";
    private static final long EXPIRATION_SECONDS = 3600L;

    private final List<ServiceAccountTokenService> services = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        services.forEach(ServiceAccountTokenService::close);
        services.clear();
    }

    /**
     * Creates the token service and registers it for clean-up after the test.
     *
     * @param kubernetesClient  Kubernetes client which should be used by the service
     *
     * @return  The token service
     */
    private ServiceAccountTokenService tokenService(KubernetesClient kubernetesClient) {
        ServiceAccountTokenService service = new ServiceAccountTokenService(kubernetesClient);
        services.add(service);
        return service;
    }

    private ServiceAccountTokenService tokenService(KubernetesClient kubernetesClient, long reaperIntervalMs) {
        ServiceAccountTokenService service = new ServiceAccountTokenService(kubernetesClient, reaperIntervalMs);
        services.add(service);
        return service;
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
        when(namespacedOp.withName(any())).thenReturn(serviceAccountResource);

        MixedOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> op = mock(MixedOperation.class);
        when(op.inNamespace(any())).thenReturn(namespacedOp);

        KubernetesClient client = mock(KubernetesClient.class);
        when(client.serviceAccounts()).thenReturn(op);

        return client;
    }

    private static ServiceAccountResource mockServiceAccountResource(TokenRequest... responses) {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);

        when(serviceAccountResource.tokenRequest(any())).thenReturn(responses[0], Arrays.copyOfRange(responses, 1, responses.length));

        return serviceAccountResource;
    }

    //////////////////////////////////////////////////
    // Tests for getting the tokens
    //////////////////////////////////////////////////

    @Test
    public void testTokenIsRequestedFromTheKubernetesApi() {
        Instant expiration = Instant.now().plusSeconds(EXPIRATION_SECONDS);
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", expiration));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        long before = System.currentTimeMillis();
        ServiceAccountToken token = service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS);
        long after = System.currentTimeMillis();

        assertThat(token.value(), is("my-token"));
        assertThat(token.expiresAtMs(), is(expiration.toEpochMilli()));
        assertThat(token.issuedAtMs(), greaterThanOrEqualTo(before));
        assertThat(token.issuedAtMs(), lessThanOrEqualTo(after));

        ArgumentCaptor<TokenRequest> requestCaptor = ArgumentCaptor.forClass(TokenRequest.class);
        verify(serviceAccountResource).tokenRequest(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getSpec().getAudiences(), is(List.of(AUDIENCE)));
        assertThat(requestCaptor.getValue().getSpec().getExpirationSeconds(), is(EXPIRATION_SECONDS));
    }

    @Test
    public void testTokenIsRequestedForTheIdentity() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        assertThat(service.token(new RequestedServiceAccountAuthIdentity(new Reconciliation("test", "Kafka", NAMESPACE, "my-cluster"), AUDIENCE, EXPIRATION_SECONDS)).value(), is("my-token"));

        ArgumentCaptor<TokenRequest> requestCaptor = ArgumentCaptor.forClass(TokenRequest.class);
        verify(serviceAccountResource).tokenRequest(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getSpec().getAudiences(), is(List.of(AUDIENCE)));
        assertThat(requestCaptor.getValue().getSpec().getExpirationSeconds(), is(EXPIRATION_SECONDS));
    }

    @Test
    public void testTokenIsCachedAndReused() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-token"));
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-token"));
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-token"));

        verify(serviceAccountResource, times(1)).tokenRequest(any());
        assertThat(service.cachedTokens(), is(1));
    }

    @Test
    public void testTokenIsCachedWhenKubernetesShortensTheRequestedExpiration() {
        // The Kubernetes API can issue the token with a shorter validity than requested. The caching has to follow the
        // returned expiration time and not the requested one, otherwise the token would be requested on every call.
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().plusSeconds(600)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-token"));
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-token"));

        verify(serviceAccountResource, times(1)).tokenRequest(any());
    }

    @Test
    public void testNewTokenIsRequestedWhenTheCachedOneIsCloseToExpiration() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(
                // The first token is already past the renewal threshold and should not be reused
                tokenRequestResponse("my-old-token", Instant.now().minusSeconds(EXPIRATION_SECONDS)),
                tokenRequestResponse("my-new-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-old-token"));
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-new-token"));
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-new-token"));

        verify(serviceAccountResource, times(2)).tokenRequest(any());
        assertThat(service.cachedTokens(), is(1));
    }

    @Test
    public void testTokensAreCachedSeparatelyForDifferentServiceAccounts() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS);
        service.token("other-namespace", SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS);
        service.token(NAMESPACE, "other-cluster-cluster-operator", AUDIENCE, EXPIRATION_SECONDS);
        service.token(NAMESPACE, SERVICE_ACCOUNT, "other-audience", EXPIRATION_SECONDS);
        service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, 600L);

        verify(serviceAccountResource, times(5)).tokenRequest(any());
        assertThat(service.cachedTokens(), is(5));
    }

    @Test
    public void testFailsWhenKubernetesApiReturnsNoResponse() {
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(mockServiceAccountResource((TokenRequest) null)));

        RuntimeException e = assertThrows(RuntimeException.class, () -> service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS));
        assertThat(e.getMessage(), is("Kubernetes API did not return a token for ServiceAccount my-namespace/my-cluster-cluster-operator"));
        assertThat(service.cachedTokens(), is(0));
    }

    @Test
    public void testFailsWhenKubernetesApiReturnsNoStatus() {
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(mockServiceAccountResource(new TokenRequestBuilder().build())));

        RuntimeException e = assertThrows(RuntimeException.class, () -> service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS));
        assertThat(e.getMessage(), is("Kubernetes API did not return a token for ServiceAccount my-namespace/my-cluster-cluster-operator"));
    }

    @Test
    public void testFailsWhenKubernetesApiReturnsNoToken() {
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(mockServiceAccountResource(tokenRequestResponse(null, Instant.now().plusSeconds(EXPIRATION_SECONDS)))));

        RuntimeException e = assertThrows(RuntimeException.class, () -> service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS));
        assertThat(e.getMessage(), is("Kubernetes API did not return a token for ServiceAccount my-namespace/my-cluster-cluster-operator"));
    }

    @Test
    public void testFailsWhenKubernetesApiReturnsNoExpiration() {
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(mockServiceAccountResource(tokenRequestResponse("my-token", null))));

        RuntimeException e = assertThrows(RuntimeException.class, () -> service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS));
        assertThat(e.getMessage(), is("Kubernetes API did not return a token for ServiceAccount my-namespace/my-cluster-cluster-operator"));
    }

    @Test
    public void testFailedTokenRequestDoesNotBreakTheService() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(
                // The first token is already past the renewal threshold, so every call requests a new token
                tokenRequestResponse("my-old-token", Instant.now().minusSeconds(EXPIRATION_SECONDS)),
                null,
                tokenRequestResponse("my-new-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-old-token"));
        assertThrows(RuntimeException.class, () -> service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS));
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-new-token"));
    }

    //////////////////////////////////////////////////
    // Tests for the reaping of the unused tokens
    //////////////////////////////////////////////////

    @Test
    public void testReaperKeepsTheUsableTokens() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().plusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS);
        assertThat(service.cachedTokens(), is(1));

        service.reap();
        assertThat(service.cachedTokens(), is(1));

        // The token is still cached => no new token request is sent
        assertThat(service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS).value(), is("my-token"));
        verify(serviceAccountResource, times(1)).tokenRequest(any());
    }

    @Test
    public void testReaperRemovesTheTokensWhichAreCloseToExpiration() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().minusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource));

        service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS);
        assertThat(service.cachedTokens(), is(1));

        service.reap();
        assertThat(service.cachedTokens(), is(0));
    }

    @Test
    public void testReaperRunsPeriodically() {
        ServiceAccountResource serviceAccountResource = mockServiceAccountResource(tokenRequestResponse("my-token", Instant.now().minusSeconds(EXPIRATION_SECONDS)));
        ServiceAccountTokenService service = tokenService(mockKubernetesClient(serviceAccountResource), 10L);

        service.token(NAMESPACE, SERVICE_ACCOUNT, AUDIENCE, EXPIRATION_SECONDS);

        TestUtils.waitFor("the token to be removed from the cache by the reaper", 10, 10_000, () -> service.cachedTokens() == 0);
    }
}
