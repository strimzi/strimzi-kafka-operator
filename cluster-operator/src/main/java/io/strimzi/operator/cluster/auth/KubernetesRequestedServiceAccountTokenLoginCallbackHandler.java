/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka SASL/OAUTHBEARER login callback handler that obtains the token for a configured Service Account in a
 * configured namespace and with a configured audience from the {@link ServiceAccountTokenService}.
 *
 * The tokens are cached by the token service and shared with the other users of the Service Account tokens such as the
 * Kafka Agent client. So a new token is minted through the Kubernetes TokenRequest API only when the cached token is
 * not usable anymore. Kafka's OAuthBearerLoginModule uses the token lifetime to schedule the next login, so it asks
 * for a token only when the one it has is close to expiring.
 *
 * Example configuration in the {@code OAuthBearerLoginModule} entry:
 * {@code
 * sasl.login.callback.handler.class=io.strimzi.operator.cluster.auth.KubernetesRequestedServiceAccountTokenLoginCallbackHandler
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

    /**
     * Default constructor — required because Kafka clients (Admin API client in our case) load the handler via reflection.
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

        LOGGER.debug("Configured Kubernetes Service Account token login for {} (audience={})", principalName, audience);
    }

    /**
     * Provides the token service used to get the tokens. Overridable so tests can inject their own instance.
     *
     * @return  Service Account token service
     */
    /* test */ ServiceAccountTokenService tokenService() {
        return ServiceAccountTokenService.getInstance();
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback tokenCallback) {
                try {
                    tokenCallback.token(oauthBearerToken());
                } catch (RuntimeException e) {
                    LOGGER.error("Failed to get Service Account token for {}", principalName, e);
                    tokenCallback.error("invalid_token", e.getMessage(), null);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private OAuthBearerToken oauthBearerToken() {
        ServiceAccountToken token = tokenService().token(namespace, serviceAccountName, audience, expirationSeconds);
        return new ServiceAccountOAuthBearerToken(token, principalName);
    }

    @Override
    public void close() {
        // The token service is shared and lives for the whole lifetime of the operator. So there is nothing to close.
    }

    private static String requiredOption(Map<String, ?> options, String key) {
        Object value = options.get(key);
        if (value == null || value.toString().isEmpty()) {
            throw new IllegalArgumentException("Required JAAS option '" + key + "' is missing or empty");
        }
        return value.toString();
    }

    private static final class ServiceAccountOAuthBearerToken implements OAuthBearerToken {
        private final ServiceAccountToken token;
        private final String principalName;

        ServiceAccountOAuthBearerToken(ServiceAccountToken token, String principalName) {
            this.token = token;
            this.principalName = principalName;
        }

        @Override
        public String value() {
            return token.value();
        }

        @Override
        public Set<String> scope() {
            return Collections.emptySet();
        }

        @Override
        public long lifetimeMs() {
            return token.expiresAtMs();
        }

        @Override
        public String principalName() {
            return principalName;
        }

        @Override
        public Long startTimeMs() {
            return token.issuedAtMs();
        }
    }
}
