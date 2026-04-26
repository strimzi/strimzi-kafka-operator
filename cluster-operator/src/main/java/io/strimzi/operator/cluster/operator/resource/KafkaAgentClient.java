/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;

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

/**
 * Creates HTTP client and interacts with Kafka Agent's REST endpoint
 */
public class KafkaAgentClient {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAgentClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKER_STATE_REST_PATH = "/v1/broker-state/";
    private static final int KAFKA_AGENT_HTTPS_PORT = 8443;
    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();
    // Bounds the connect and full HTTP request lifecycle so that a broker which accepts the TCP connection but
    // never produces a response (e.g. alive but stuck on IO) cannot block the KafkaRoller's single-threaded
    // executor indefinitely. The Kafka Agent only serves a small broker-state JSON, so 10 seconds is well above
    // the expected response time on a healthy broker yet small enough to keep the roller responsive.
    /* test */ static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private final String namespace;
    private final Reconciliation reconciliation;
    private final String cluster;
    private TlsPemIdentity tlsPemIdentity;
    private HttpClient httpClient;

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param cluster   Cluster name
     * @param namespace Cluster namespace
     * @param tlsPemIdentity Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     */
    public KafkaAgentClient(Reconciliation reconciliation, String cluster, String namespace, TlsPemIdentity tlsPemIdentity) {
        this.reconciliation = reconciliation;
        this.cluster = cluster;
        this.namespace = namespace;
        this.tlsPemIdentity = tlsPemIdentity;
        this.httpClient = createHttpClient();
    }

    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param cluster   Cluster name
     * @param namespace Cluster namespace
     */
    public KafkaAgentClient(Reconciliation reconciliation, String cluster, String namespace) {
        this.reconciliation = reconciliation;
        this.namespace = namespace;
        this.cluster =  cluster;
    }

    private HttpClient createHttpClient() {
        if (tlsPemIdentity == null) {
            throw new RuntimeException("Missing cluster CA and operator certificates required to create connection to Kafka Agent");
        }

        try {
            if (tlsPemIdentity.pemTrustSet() == null) {
                throw new RuntimeException("Missing cluster CA trust set certificates required to create connection to Kafka Agent");
            }
            String trustManagerFactoryAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
            trustManagerFactory.init(tlsPemIdentity.pemTrustSet().trustStore());

            if (tlsPemIdentity.pemAuthIdentity() == null) {
                throw new RuntimeException("Missing cluster operator authentication identity certificates required to create connection to Kafka Agent");
            }
            String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithm);
            keyManagerFactory.init(tlsPemIdentity.pemAuthIdentity().keyStore(KEYSTORE_PASSWORD), KEYSTORE_PASSWORD);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

            return httpClientBuilder()
                    .sslContext(sslContext)
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Failed to configure HTTP client", e);
        }
    }

    /**
     * Returns an {@link HttpClient.Builder} pre-configured with {@link #HTTP_REQUEST_TIMEOUT} as the connect timeout.
     * Centralising the timeout wiring here keeps {@link #createHttpClient()} focused on TLS setup and lets tests
     * assert that the configured connect timeout is applied without needing a TLS identity.
     *
     * @return an {@link HttpClient.Builder} with the connect timeout already applied
     */
    /* test */ static HttpClient.Builder httpClientBuilder() {
        return HttpClient.newBuilder().connectTimeout(HTTP_REQUEST_TIMEOUT);
    }

    /**
     * Builds a {@code GET} {@link HttpRequest} for the given URI with {@link #HTTP_REQUEST_TIMEOUT} applied as the
     * full request timeout. Centralising the timeout wiring here lets tests assert the request timeout without
     * needing a live HTTP endpoint.
     *
     * @param uri   the target URI for the request
     *
     * @return a {@code GET} {@link HttpRequest} for {@code uri} with the request timeout already applied
     */
    /* test */ static HttpRequest buildRequest(URI uri) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .GET()
                .build();
    }

    String doGet(URI uri) {
        try {
            HttpRequest req = buildRequest(uri);

            var response = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
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
            URI uri = new URI("https", null, host, KAFKA_AGENT_HTTPS_PORT, BROKER_STATE_REST_PATH, null, null);
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
}
