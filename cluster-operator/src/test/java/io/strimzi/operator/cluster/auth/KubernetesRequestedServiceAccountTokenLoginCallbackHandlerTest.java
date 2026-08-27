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
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:NoFullyQualifiedClassNames") // False positive, fully qualified class name used in a string
public class KubernetesRequestedServiceAccountTokenLoginCallbackHandlerTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String SERVICE_ACCOUNT = "my-cluster-cluster-operator";
    private static final String AUDIENCE = "strimzi.io/kafka/my-namespace/my-cluster";
    private static final String PRINCIPAL = "User:system:serviceaccount:my-namespace:my-cluster-cluster-operator";
    private static final String EXPIRATION_TIMESTAMP = "2026-08-27T17:00:00Z";

    /**
     * Handler subclass which injects a mocked Kubernetes client instead of building a real one.
     */
    private static class MockedHandler extends KubernetesRequestedServiceAccountTokenLoginCallbackHandler {
        private final KubernetesClient client;

        MockedHandler(KubernetesClient client) {
            this.client = client;
        }

        @Override
        KubernetesClient buildKubernetesClient() {
            return client;
        }
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.NAMESPACE_CONFIG, NAMESPACE);
        options.put(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.SERVICE_ACCOUNT_CONFIG, SERVICE_ACCOUNT);
        options.put(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.AUDIENCE_CONFIG, AUDIENCE);
        options.put(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.EXPIRATION_SECONDS_CONFIG, "3600");
        return options;
    }

    private static List<AppConfigurationEntry> jaasConfigEntries(Map<String, String> options) {
        return List.of(new AppConfigurationEntry("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options));
    }

    @SuppressWarnings("unchecked")
    private static KubernetesClient mockKubernetesClient(ServiceAccountResource serviceAccountResource) {
        NonNamespaceOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> namespacedOp = mock(NonNamespaceOperation.class);
        when(namespacedOp.withName(SERVICE_ACCOUNT)).thenReturn(serviceAccountResource);

        MixedOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> op = mock(MixedOperation.class);
        when(op.inNamespace(NAMESPACE)).thenReturn(namespacedOp);

        KubernetesClient client = mock(KubernetesClient.class);
        when(client.serviceAccounts()).thenReturn(op);

        return client;
    }

    private static KubernetesClient mockKubernetesClient(TokenRequest tokenRequestResponse) {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse);
        return mockKubernetesClient(serviceAccountResource);
    }

    private static TokenRequest tokenRequestResponse(String token, String expirationTimestamp) {
        return new TokenRequestBuilder()
                .withNewStatus()
                    .withToken(token)
                    .withExpirationTimestamp(expirationTimestamp)
                .endStatus()
                .build();
    }

    private static KubernetesRequestedServiceAccountTokenLoginCallbackHandler configuredHandler(KubernetesClient client) {
        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = new MockedHandler(client);
        handler.configure(Map.of(), "OAUTHBEARER", jaasConfigEntries(options()));
        return handler;
    }

    //////////////////////////////////////////////////
    // Tests for the configure method
    //////////////////////////////////////////////////

    @Test
    public void testConfigureWithoutJaasConfigEntries() {
        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = new KubernetesRequestedServiceAccountTokenLoginCallbackHandler();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> handler.configure(Map.of(), "OAUTHBEARER", null));
        assertThat(e.getMessage(), is("No JAAS configuration entry found for io.strimzi.operator.cluster.auth.KubernetesRequestedServiceAccountTokenLoginCallbackHandler"));

        e = assertThrows(IllegalArgumentException.class, () -> handler.configure(Map.of(), "OAUTHBEARER", List.of()));
        assertThat(e.getMessage(), is("No JAAS configuration entry found for io.strimzi.operator.cluster.auth.KubernetesRequestedServiceAccountTokenLoginCallbackHandler"));
    }

    @Test
    public void testConfigureWithMissingOption() {
        for (String option : List.of(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.NAMESPACE_CONFIG,
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.SERVICE_ACCOUNT_CONFIG,
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.AUDIENCE_CONFIG,
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.EXPIRATION_SECONDS_CONFIG)) {
            Map<String, String> options = options();
            options.remove(option);

            KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = new KubernetesRequestedServiceAccountTokenLoginCallbackHandler();
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> handler.configure(Map.of(), "OAUTHBEARER", jaasConfigEntries(options)));
            assertThat(e.getMessage(), is("Required JAAS option '" + option + "' is missing or empty"));
        }
    }

    @Test
    public void testConfigureWithEmptyOption() {
        Map<String, String> options = options();
        options.put(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.AUDIENCE_CONFIG, "");

        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = new KubernetesRequestedServiceAccountTokenLoginCallbackHandler();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> handler.configure(Map.of(), "OAUTHBEARER", jaasConfigEntries(options)));
        assertThat(e.getMessage(), is("Required JAAS option 'strimzi.kubernetes.token.audience' is missing or empty"));
    }

    //////////////////////////////////////////////////
    // Tests for the handle method
    //////////////////////////////////////////////////

    @Test
    public void testHandleMintsToken() throws Exception {
        ServiceAccountResource serviceAccountResource = mock(ServiceAccountResource.class);
        when(serviceAccountResource.tokenRequest(any())).thenReturn(tokenRequestResponse("my-token", EXPIRATION_TIMESTAMP));

        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = configuredHandler(mockKubernetesClient(serviceAccountResource));

        long before = System.currentTimeMillis();
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{callback});
        long after = System.currentTimeMillis();

        // The token handed to Kafka
        OAuthBearerToken token = callback.token();
        assertThat(callback.errorCode(), is(nullValue()));
        assertThat(token.value(), is("my-token"));
        assertThat(token.principalName(), is(PRINCIPAL));
        assertThat(token.lifetimeMs(), is(Instant.parse(EXPIRATION_TIMESTAMP).toEpochMilli()));
        assertThat(token.scope(), is(emptyIterable()));
        assertThat(token.startTimeMs(), greaterThanOrEqualTo(before));
        assertThat(token.startTimeMs(), lessThanOrEqualTo(after));

        // The TokenRequest sent to the Kubernetes API
        ArgumentCaptor<TokenRequest> requestCaptor = ArgumentCaptor.forClass(TokenRequest.class);
        verify(serviceAccountResource).tokenRequest(requestCaptor.capture());
        TokenRequest request = requestCaptor.getValue();
        assertThat(request.getSpec().getAudiences(), is(List.of(AUDIENCE)));
        assertThat(request.getSpec().getExpirationSeconds(), is(3600L));
    }

    @Test
    public void testHandleWithNullResponse() throws Exception {
        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = configuredHandler(mockKubernetesClient((TokenRequest) null));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{callback});

        assertThat(callback.token(), is(nullValue()));
        assertThat(callback.errorCode(), is("invalid_token"));
        assertThat(callback.errorDescription(), is("Kubernetes API did not return a token for ServiceAccount my-namespace/my-cluster-cluster-operator"));
    }

    @Test
    public void testHandleWithResponseWithoutStatus() throws Exception {
        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = configuredHandler(mockKubernetesClient(new TokenRequestBuilder().build()));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{callback});

        assertThat(callback.token(), is(nullValue()));
        assertThat(callback.errorCode(), is("invalid_token"));
    }

    @Test
    public void testHandleWithResponseWithoutToken() throws Exception {
        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = configuredHandler(mockKubernetesClient(tokenRequestResponse(null, EXPIRATION_TIMESTAMP)));

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{callback});

        assertThat(callback.token(), is(nullValue()));
        assertThat(callback.errorCode(), is("invalid_token"));
    }

    @Test
    public void testHandleWithUnsupportedCallback() {
        KubernetesRequestedServiceAccountTokenLoginCallbackHandler handler = configuredHandler(mockKubernetesClient(tokenRequestResponse("my-token", EXPIRATION_TIMESTAMP)));

        NameCallback callback = new NameCallback("Username:");
        UnsupportedCallbackException e = assertThrows(UnsupportedCallbackException.class, () -> handler.handle(new Callback[]{callback}));
        assertThat(e.getCallback(), is(callback));
    }

    //////////////////////////////////////////////////
    // Tests for the close method
    //////////////////////////////////////////////////

    @Test
    public void testCloseClosesTheClient() {
        KubernetesClient client = mockKubernetesClient(tokenRequestResponse("my-token", EXPIRATION_TIMESTAMP));

        configuredHandler(client).close();

        verify(client).close();
    }

    @Test
    public void testCloseWithoutConfigure() {
        // Kafka can close a handler which failed to configure => this should not throw
        new MockedHandler(mock(KubernetesClient.class)).close();
    }
}
