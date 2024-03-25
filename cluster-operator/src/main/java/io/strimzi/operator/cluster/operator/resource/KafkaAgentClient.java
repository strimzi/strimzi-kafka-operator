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
import io.strimzi.operator.common.Util;
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
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

/**
 * Creates HTTP client and interacts with Kafka Agent's REST endpoint
 */
public class KafkaAgentClient {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAgentClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BROKER_STATE_REST_PATH = "/v1/broker-state/";
    private static final String KRAFT_MIGRATION_PATH = "/v1/kraft-migration/";
    private static final int KAFKA_AGENT_HTTPS_PORT = 8443;
    private static final String KEYSTORE_TYPE_JKS = "JKS";
    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();
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
            String trustManagerFactoryAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
            trustManagerFactory.init(getTrustStore());

            String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithm);
            keyManagerFactory.init(getKeyStore(), KEYSTORE_PASSWORD);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

            return HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Failed to configure HTTP client", e);
        }
    }

    private KeyStore getTrustStore() throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
        if (tlsPemIdentity.pemTrustSet() == null) {
            throw new RuntimeException("Missing cluster CA trust set certificates required to create connection to Kafka Agent");
        }
        KeyStore trustStore = KeyStore.getInstance(KEYSTORE_TYPE_JKS);
        trustStore.load(null);
        int aliasIndex = 0;
        for (X509Certificate certificate : tlsPemIdentity.pemTrustSet().trustedCertificates()) {
            trustStore.setEntry(certificate.getSubjectX500Principal().getName() + "-" + aliasIndex, new KeyStore.TrustedCertificateEntry(certificate), null);
            aliasIndex++;
        }
        return trustStore;
    }

    private KeyStore getKeyStore() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        if (tlsPemIdentity.pemAuthIdentity() == null) {
            throw new RuntimeException("Missing cluster operator authentication identity certificates required to create connection to Kafka Agent");
        }
        byte[] decodedKey = Util.decodePemPrivateKey(tlsPemIdentity.pemAuthIdentity().privateKeyAsPem());
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodedKey);
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        final PrivateKey key = keyFactory.generatePrivate(keySpec);

        KeyStore coKeyStore = KeyStore.getInstance(KEYSTORE_TYPE_JKS);
        coKeyStore.load(null);
        coKeyStore.setKeyEntry("cluster-operator", key, KEYSTORE_PASSWORD, new Certificate[]{tlsPemIdentity.pemAuthIdentity().certificateChain()});

        return coKeyStore;
    }

    String doGet(URI uri) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

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

    /**
     * Gets ZooKeeper to KRaft migration state by sending HTTP request to the /v1/kraft-migration endpoint of the KafkaAgent
     *
     * @param podName Name of the pod to interact with
     * @return  ZooKeeper to KRaft migration state
     */
    public KRaftMigrationState getKRaftMigrationState(String podName) {
        KRaftMigrationState kraftMigrationState = new KRaftMigrationState(-1);
        String host = DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName);
        try {
            URI uri = new URI("https", null, host, KAFKA_AGENT_HTTPS_PORT, KRAFT_MIGRATION_PATH, null, null);
            kraftMigrationState = MAPPER.readValue(doGet(uri), KRaftMigrationState.class);
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to parse ZooKeeper to KRaft migration state", e);
        } catch (URISyntaxException e) {
            LOGGER.warnCr(reconciliation, "Failed to get ZooKeeper to KRaft migration state due to invalid URI", e);
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "Failed to get ZooKeeper to KRaft migration state", e);
        }
        return kraftMigrationState;
    }
}
