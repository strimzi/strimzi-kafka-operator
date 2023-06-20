/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import java.util.Base64;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;

/**
 * Creates HTTP client and interacts with Kafka Agent's REST endpoint
 */
public class KafkaAgentClient {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAgentClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKER_STATE_REST_PATH = "/v1/broker-state";
    private static final int BROKER_STATE_HTTPS_PORT = 8443;

    private String namespace;
    private Reconciliation reconciliation;
    private String cluster;
    private Secret clusterCaCertSecret;
    private Secret coKeySecret;

    KafkaAgentClient(Reconciliation reconciliation, String cluster, String namespace, Secret clusterCaCertSecret, Secret coKeySecret) {
        this.reconciliation = reconciliation;
        this.cluster = cluster;
        this.namespace = namespace;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
    }

    protected String doGet(String host) {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(new ByteArrayInputStream(
                Util.decodeFromSecret(clusterCaCertSecret, "ca.12")),
                new String(Base64.getDecoder().decode(Util.decodeFromSecret(clusterCaCertSecret, "ca.password")), StandardCharsets.US_ASCII).toCharArray()
            );
            String trustManagerFactoryAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
            trustManagerFactory.init(trustStore);

            char[] keyPassword = new String(Base64.getDecoder().decode(Util.decodeFromSecret(coKeySecret, "cluster-operator.password")), StandardCharsets.US_ASCII).toCharArray();
            KeyStore coKeyStore = KeyStore.getInstance("PKCS12");
            coKeyStore.load(new ByteArrayInputStream(
                Util.decodeFromSecret(clusterCaCertSecret, "cluster-operator.p12")),
                keyPassword
            );
            String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithm);
            keyManagerFactory.init(coKeyStore, keyPassword);

            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(host + ":" + BROKER_STATE_HTTPS_PORT + BROKER_STATE_REST_PATH))
                    .GET()
                    .build();

            var response =  HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .build()
                    .send(req, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Unexpected HTTP status code: " + response.statusCode());
            }
            return response.body();
        } catch (GeneralSecurityException | IOException | URISyntaxException | InterruptedException e) {
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
            brokerstate = MAPPER.readValue(doGet(host), BrokerState.class);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, "Failed to get broker state", e);
        }
        return brokerstate;
    }
}
