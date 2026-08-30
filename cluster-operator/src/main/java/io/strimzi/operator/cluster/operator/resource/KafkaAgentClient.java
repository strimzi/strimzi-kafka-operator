/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.authentication.TokenRequest;
import io.fabric8.kubernetes.api.model.authentication.TokenRequestBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.auth.RequestedServiceAccountAuthIdentity;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.Identity;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;

/**
 * Creates HTTP client and interacts with Kafka Agent's REST endpoint
 */
public class KafkaAgentClient {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAgentClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKER_STATE_REST_PATH = "/v1/broker-state/";
    private static final int KAFKA_AGENT_HTTPS_PORT = 8443;
    // Bounds the connect and full HTTP request lifecycle so that a broker which accepts the TCP connection but
    // never produces a response (e.g. alive but stuck on IO) cannot block the KafkaRoller's single-threaded
    // executor indefinitely. The Kafka Agent only serves a small broker-state JSON, so 10 seconds is well above
    // the expected response time on a healthy broker yet small enough to keep the roller responsive.
    private static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    // Fraction of the token lifetime after which the token is renewed.
    private static final double TOKEN_RENEWAL_THRESHOLD = 0.8;

    private final String namespace;
    private final Reconciliation reconciliation;
    private final String cluster;
    private final Identity identity;
    private final KubernetesClient kubernetesClient;
    private final HttpClient httpClient;

    private String cachedToken;
    private long cachedTokenRefreshAt;

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param cluster           Cluster name
     * @param namespace         Cluster namespace
     * @param identity          Trust set and identity for authentication for connecting to the Kafka cluster
     * @param kubernetesClient  Kubernetes client used to get the token for the service account
     */
    public KafkaAgentClient(Reconciliation reconciliation, String cluster, String namespace, Identity identity, KubernetesClient kubernetesClient) {
        this.reconciliation = reconciliation;
        this.cluster = cluster;
        this.namespace = namespace;
        this.identity = identity;
        this.kubernetesClient = kubernetesClient;
        this.httpClient = createHttpClient();
    }

    private HttpClient createHttpClient() {
        if (identity == null) {
            throw new RuntimeException("Missing cluster CA and operator certificates required to create connection to Kafka Agent");
        }

        try {
            HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();

            // If TLS encryption is enabled (we have PemTrustSet), we configure SSL for HTTP Client
            if (identity.trustSet() instanceof PemTrustSet pemTrustSet) {
                String trustManagerFactoryAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
                trustManagerFactory.init(pemTrustSet.trustStore());

                // If TLS client authentication is enabled (we have PemAuthIdentity), we configure it on top of the TLS encryption
                KeyManagerFactory keyManagerFactory = null;
                if (identity.authIdentity() instanceof PemAuthIdentity pemAuthIdentity) {
                    String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
                    keyManagerFactory = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithm);
                    keyManagerFactory.init(pemAuthIdentity.keyStore(), null);
                }

                SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
                sslContext.init(
                        keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null,
                        trustManagerFactory.getTrustManagers(),
                        null
                );

                // Configure the SslContext for the HTTP Client
                httpClientBuilder = httpClientBuilder.sslContext(sslContext);
            }

            return httpClientBuilder
                    .connectTimeout(HTTP_REQUEST_TIMEOUT)
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Failed to configure HTTP client", e);
        }
    }

    String doGet(URI uri) {
        try {
            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(HTTP_REQUEST_TIMEOUT)
                    .GET();

            if (identity.authIdentity() instanceof RequestedServiceAccountAuthIdentity authIdentity) {
                reqBuilder.header("Authorization", "Bearer " + currentToken(authIdentity));
            }

            var response = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Unexpected HTTP status code: " + response.statusCode());
            }
            return response.body();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to send HTTP request to Kafka Agent", e);
        }
    }

    /**
     * Gets broker state by sending HTTP request to the /v1/broker-state endpoint of the KafkaAgent
     *
     * @param podName Name of the pod to interact with
     * @return A BrokerState that contains broker state and recovery progress.
     *         -1 is returned for broker state if the http request failed or returned non 200 response.
     *         Null value is returned for recovery progress if broker state is not 2 (RECOVERY).
     */
    public BrokerState getBrokerState(String podName) {
        BrokerState brokerstate = new BrokerState(-1, null);
        String host = DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName);
        try {
            URI uri = new URI(identity.trustSet() instanceof PemTrustSet ? "https" : "http", null, host, KAFKA_AGENT_HTTPS_PORT, BROKER_STATE_REST_PATH, null, null);
            brokerstate = MAPPER.readValue(doGet(uri), BrokerState.class);
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to parse broker state", e);
        } catch (URISyntaxException e) {
            LOGGER.warnCr(reconciliation, "Failed to get broker state due to invalid URI", e);
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "Failed to get broker state", e);
        }
        return brokerstate;
    }

    /**
     * Returns a valid Service Account token for the per-cluster cluster-operator SA, minting a fresh one via the
     * Kubernetes TokenRequest API when no cached token is available, or when the cached one should be refreshed.
     *
     * @param authIdentity  The RequestedServiceAccountAuthIdentity containing the authentication details
     *
     * @return  JWT token string suitable for use in an HTTP Authorization Bearer header
     */
    synchronized String currentToken(RequestedServiceAccountAuthIdentity authIdentity) {
        // If we do not have the token yet or it should be refreshed, we get the new token from Kube API
        if (cachedToken == null || System.currentTimeMillis() >= cachedTokenRefreshAt) {
            TokenRequest request = new TokenRequestBuilder()
                    .withNewSpec()
                        .withAudiences(authIdentity.audience())
                        .withExpirationSeconds(authIdentity.expirationSeconds())
                    .endSpec()
                    .build();
            TokenRequest response = kubernetesClient.serviceAccounts()
                    .inNamespace(authIdentity.namespace())
                    .withName(authIdentity.serviceAccountName())
                    .tokenRequest(request);

            if (response == null || response.getStatus() == null || response.getStatus().getToken() == null) {
                throw new RuntimeException("Kubernetes API did not return a token for ServiceAccount " + authIdentity.namespace() + "/" + authIdentity.serviceAccountName());
            }

            cachedToken = response.getStatus().getToken();

            // The token refresh time is computed from its expiration time to refresh it before it is expired.
            long now = System.currentTimeMillis();
            long expiresAt = Instant.parse(response.getStatus().getExpirationTimestamp()).toEpochMilli();
            cachedTokenRefreshAt = now + (long) (TOKEN_RENEWAL_THRESHOLD * (expiresAt - now));
        }

        return cachedToken;
    }
}
