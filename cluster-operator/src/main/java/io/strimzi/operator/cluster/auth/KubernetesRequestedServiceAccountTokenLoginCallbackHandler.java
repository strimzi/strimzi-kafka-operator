/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka SASL/OAUTHBEARER login callback handler that mints a fresh token via the Kubernetes TokenRequest API on each
 * invocation. The token is bound to a configured Service Account in a configured namespace, with a configured audience.
 *
 * There is currently no caching done internally. Kafka's OAuthBearerLoginModule uses the token lifetime to schedule the
 * next refresh, so a new TokenRequest is only made when the prior token is close to expiring.
 *
 * Example configuration in the {@code OAuthBearerLoginModule} entry:
 * {@code
 * sasl.login.callback.handler.class=io.strimzi.operator.cluster.model.auth.KubernetesRequestedServiceAccountTokenLoginCallbackHandler
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
 *     strimzi.kubernetes.token.namespace="my-kafka-ns" \
 *     strimzi.kubernetes.token.serviceaccount="my-cluster-cluster-operator" \
 *     strimzi.kubernetes.token.audience="strimzi.io" \
 *     strimzi.kubernetes.token.expiration.seconds="3600";
 * }
 */
public class KubernetesRequestedServiceAccountTokenLoginCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOGGER = LogManager.getLogger(KubernetesRequestedServiceAccountTokenLoginCallbackHandler.class);

    /**
     * Namespace containing the Service Account to mint tokens for.
     */
    public static final String NAMESPACE_CONFIG = "strimzi.kubernetes.token.namespace";

    /**
     * Name of the Service Account to mint tokens for.
     */
    public static final String SERVICE_ACCOUNT_CONFIG = "strimzi.kubernetes.token.serviceaccount";

    /**
     * Audience claim to request on the token.
     */
    public static final String AUDIENCE_CONFIG = "strimzi.kubernetes.token.audience";

    /**
     * Requested token lifetime in seconds.
     */
    public static final String EXPIRATION_SECONDS_CONFIG = "strimzi.kubernetes.token.expiration.seconds";

    private String namespace;
    private String serviceAccountName;
    private String audience;
    private long expirationSeconds;
    private String principalName;
    private KubernetesClient client;

    /**
     * Default constructor — required because Kafka loads the handler via reflection.
     */
    public KubernetesRequestedServiceAccountTokenLoginCallbackHandler() {
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (jaasConfigEntries == null || jaasConfigEntries.isEmpty()) {
            throw new IllegalArgumentException("No JAAS configuration entry found for " + getClass().getName());
        }
        Map<String, ?> options = jaasConfigEntries.get(0).getOptions();

        namespace = requiredOption(options, NAMESPACE_CONFIG);
        serviceAccountName = requiredOption(options, SERVICE_ACCOUNT_CONFIG);
        audience = requiredOption(options, AUDIENCE_CONFIG);
        expirationSeconds = Long.parseLong(requiredOption(options, EXPIRATION_SECONDS_CONFIG));
        principalName = "User:system:serviceaccount:" + namespace + ":" + serviceAccountName;

        client = buildKubernetesClient();
        LOGGER.debug("Configured Kubernetes Service Account token login for {} (audience={})", principalName, audience);
    }

    /**
     * Factory for the Kubernetes client. Overridable so tests can inject a mock.
     *
     * @return  Kubernetes client used to call the TokenRequest API
     */
    /* test */ KubernetesClient buildKubernetesClient() {
        return new KubernetesClientBuilder().withConfig(new ConfigBuilder().withUserAgent("strimzi-auth-token-callback").build()).build();
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback tokenCallback) {
                try {
                    tokenCallback.token(mintToken());
                } catch (RuntimeException e) {
                    LOGGER.error("Failed to mint Service Account token for {}", principalName, e);
                    tokenCallback.error("invalid_token", e.getMessage(), null);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private OAuthBearerToken mintToken() {
        TokenRequest request = new TokenRequestBuilder()
                .withNewSpec()
                .withAudiences(audience)
                .withExpirationSeconds(expirationSeconds)
                .endSpec()
                .build();
        TokenRequest response = client.serviceAccounts()
                .inNamespace(namespace)
                .withName(serviceAccountName)
                .tokenRequest(request);

        if (response == null || response.getStatus() == null || response.getStatus().getToken() == null) {
            throw new IllegalStateException("Kubernetes API did not return a token for ServiceAccount " + namespace + "/" + serviceAccountName);
        }

        String tokenValue = response.getStatus().getToken();
        long lifetimeMs = Instant.parse(response.getStatus().getExpirationTimestamp()).toEpochMilli();
        long startTimeMs = System.currentTimeMillis();
        return new ServiceAccountToken(tokenValue, principalName, lifetimeMs, startTimeMs);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    private static String requiredOption(Map<String, ?> options, String key) {
        Object value = options.get(key);
        if (value == null || value.toString().isEmpty()) {
            throw new IllegalArgumentException("Required JAAS option '" + key + "' is missing or empty");
        }
        return value.toString();
    }

    private static final class ServiceAccountToken implements OAuthBearerToken {
        private final String token;
        private final String principalName;
        private final long lifetimeMs;
        private final long startTimeMs;

        ServiceAccountToken(String token, String principalName, long lifetimeMs, long startTimeMs) {
            this.token = token;
            this.principalName = principalName;
            this.lifetimeMs = lifetimeMs;
            this.startTimeMs = startTimeMs;
        }

        @Override
        public String value() {
            return token;
        }

        @Override
        public Set<String> scope() {
            return Collections.emptySet();
        }

        @Override
        public long lifetimeMs() {
            return lifetimeMs;
        }

        @Override
        public String principalName() {
            return principalName;
        }

        @Override
        public Long startTimeMs() {
            return startTimeMs;
        }
    }
}