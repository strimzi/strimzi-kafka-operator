/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provides Service Account tokens obtained from the Kubernetes TokenRequest API and caches them for reuse.
 *
 * The tokens are used to authenticate the operator against the operands (Kafka brokers or the Kafka Agent). They are
 * typically valid for one hour, but they are needed in every reconciliation which happens by default every two
 * minutes. Without caching, every reconciliation of every cluster would create several new tokens. So the tokens are
 * cached and shared by all components which need them.
 *
 * The tokens are cached per Service Account (and per audience and requested expiration) and reused until 80% of their
 * lifetime elapses. Once that happens, the token is thrown away and a new one is requested when it is needed again.
 * The tokens are never renewed proactively. That keeps the number of the token requests at minimum and makes sure we
 * do not keep requesting tokens for clusters which were deleted or reconfigured to not use the Service Account
 * authentication anymore. The unused tokens are removed from the cache by a reaper thread, so that the cache does not
 * grow forever.
 *
 * As this class is a singleton, it is used to share the tokens between the Kafka Agent client and the Kafka Admin API
 * clients. The Admin API clients get the tokens through the SASL login callback handler which is instantiated by the
 * Kafka clients using reflection and cannot get the token service injected. The singleton is initialized lazily,
 * because in many cases the Service Account authentication is not used at all and no tokens are needed.
 */
public class ServiceAccountTokenService {
    private static final Logger LOGGER = LogManager.getLogger(ServiceAccountTokenService.class);

    /**
     * Fraction of the token lifetime after which the token is not used anymore and is removed from the cache. Most
     * services expect the token to be renewed at 80% of their lifetime, so we use a slightly lower threshold to be on
     * the safe side and try to renew it before they ask for it.
     */
    /* test */ static final double RENEWAL_THRESHOLD = 0.75;

    /**
     * Interval in which the reaper thread checks the cache for tokens which are not usable anymore. The minimal token
     * expiration allowed by our API is 600 seconds, so this interval is short enough to not keep the unused tokens in
     * the cache for a significant part of their lifetime.
     */
    private static final long REAPER_INTERVAL_MS = 60_000L;

    private static volatile ServiceAccountTokenService instance;

    private final KubernetesClient kubernetesClient;
    private final Map<TokenKey, ServiceAccountToken> tokens = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper;

    /**
     * Constructor
     *
     * @param kubernetesClient  Kubernetes client used to call the TokenRequest API
     */
    /* test */ ServiceAccountTokenService(KubernetesClient kubernetesClient) {
        this(kubernetesClient, REAPER_INTERVAL_MS);
    }

    /**
     * Constructor
     *
     * @param kubernetesClient      Kubernetes client used to call the TokenRequest API
     * @param reaperIntervalMs      Interval in which the unusable tokens are removed from the cache
     */
    /* test */ ServiceAccountTokenService(KubernetesClient kubernetesClient, long reaperIntervalMs) {
        this.kubernetesClient = kubernetesClient;
        this.reaper = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "service-account-token-reaper");
            thread.setDaemon(true);
            return thread;
        });
        this.reaper.scheduleAtFixedRate(this::reapSafely, reaperIntervalMs, reaperIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the singleton instance of the token service and creates it if it does not exist yet. The instance is
     * created lazily, so that we do not create the Kubernetes client and the reaper thread in the many cases when the
     * Service Account authentication is not used at all.
     *
     * @return  The token service instance
     */
    public static ServiceAccountTokenService getInstance() {
        ServiceAccountTokenService result = instance;

        if (result == null) {
            synchronized (ServiceAccountTokenService.class) {
                result = instance;

                if (result == null) {
                    LOGGER.info("Initializing the Service Account token service");
                    result = new ServiceAccountTokenService(new OperatorKubernetesClientBuilder("strimzi-cluster-operator", ServiceAccountTokenService.class.getPackage().getImplementationVersion()).build());
                    instance = result;
                }
            }
        }

        return result;
    }

    /**
     * Returns a token for the Service Account used by the given identity. A cached token is returned when it is still
     * usable. Otherwise, a new token is requested from the Kubernetes API.
     *
     * @param identity  Identity for which the token should be returned
     *
     * @return  The Service Account token
     */
    public ServiceAccountToken token(RequestedServiceAccountAuthIdentity identity) {
        return token(identity.namespace(), identity.serviceAccountName(), identity.audience(), identity.expirationSeconds());
    }

    /**
     * Returns a token for the given Service Account. A cached token is returned when it is still usable. Otherwise, a
     * new token is requested from the Kubernetes API.
     *
     * @param namespace             Namespace of the Service Account
     * @param serviceAccountName    Name of the Service Account
     * @param audience              Audience which should be set in the token
     * @param expirationSeconds     Expiration time of the token in seconds
     *
     * @return  The Service Account token
     */
    public ServiceAccountToken token(String namespace, String serviceAccountName, String audience, long expirationSeconds) {
        long now = System.currentTimeMillis();

        // The token is requested inside the compute method, so that we do not send multiple parallel token requests
        // for the same Service Account. Requesting the token blocks other operations with the same key. But as the
        // tokens are requested only once per ~80% of their lifetime, the blocking is very rare and very short.
        return tokens.compute(
                new TokenKey(namespace, serviceAccountName, audience, expirationSeconds),
                (key, cached) -> cached != null && cached.isUsableAt(now, RENEWAL_THRESHOLD) ? cached : requestToken(key));
    }

    /**
     * Requests a new token from the Kubernetes TokenRequest API.
     *
     * @param key   Key describing the token which should be requested
     *
     * @return  The new Service Account token
     */
    private ServiceAccountToken requestToken(TokenKey key) {
        LOGGER.debug("Requesting new token for Service Account {}/{} with audience {}", key.namespace(), key.serviceAccountName(), key.audience());

        TokenRequest request = new TokenRequestBuilder()
                .withNewSpec()
                    .withAudiences(key.audience())
                    .withExpirationSeconds(key.expirationSeconds())
                .endSpec()
                .build();
        TokenRequest response = kubernetesClient.serviceAccounts()
                .inNamespace(key.namespace())
                .withName(key.serviceAccountName())
                .tokenRequest(request);

        if (response == null || response.getStatus() == null || response.getStatus().getToken() == null || response.getStatus().getExpirationTimestamp() == null) {
            throw new RuntimeException("Kubernetes API did not return a token for ServiceAccount " + key.namespace() + "/" + key.serviceAccountName());
        }

        // The expiration time is taken from the response and not from the request, because the Kubernetes API might
        // issue the token with a shorter validity than we requested.
        return new ServiceAccountToken(response.getStatus().getToken(), System.currentTimeMillis(), Instant.parse(response.getStatus().getExpirationTimestamp()).toEpochMilli());
    }

    /**
     * Removes the tokens which are not usable anymore from the cache. The tokens are not renewed, so the tokens of the
     * clusters which are not reconciled anymore (for example because they were deleted) are simply removed and the
     * cache does not grow forever.
     */
    /* test */ void reap() {
        long now = System.currentTimeMillis();

        tokens.entrySet().removeIf(entry -> {
            if (!entry.getValue().isUsableAt(now, RENEWAL_THRESHOLD)) {
                LOGGER.debug("Removing the expiring token of the Service Account {}/{} with audience {} from the cache", entry.getKey().namespace(), entry.getKey().serviceAccountName(), entry.getKey().audience());
                return true;
            } else {
                return false;
            }
        });
    }

    /**
     * Wraps the reaping in a try-catch block. Without it, any error thrown from the reaper would silently cancel the
     * scheduled task and the tokens would be never removed from the cache again.
     */
    private void reapSafely() {
        try {
            reap();
        } catch (Throwable e) {
            LOGGER.warn("Failed to clean up the Service Account token cache", e);
        }
    }

    /**
     * Returns the number of the tokens currently held in the cache. Used by tests to check the caching and reaping.
     *
     * @return  Number of cached tokens
     */
    /* test */ int cachedTokens() {
        return tokens.size();
    }

    /**
     * Closes the token service and its resources. In the operator, the token service lives for the whole lifetime of
     * the JVM. So this is used only from the tests.
     */
    /* test */ void close() {
        reaper.shutdownNow();
        kubernetesClient.close();
    }

    /**
     * Key under which the tokens are cached. Tokens differing in any of these fields cannot be shared.
     *
     * @param namespace             Namespace of the Service Account
     * @param serviceAccountName    Name of the Service Account
     * @param audience              Audience set in the token
     * @param expirationSeconds     Requested expiration time of the token in seconds
     */
    private record TokenKey(String namespace, String serviceAccountName, String audience, long expirationSeconds) { }
}
