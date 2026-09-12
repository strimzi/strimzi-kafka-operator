/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.core.Gauge;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAgentTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CA_CERT_SECRET_NAME = "my-cluster-ca-cert";
    private static final String NODE_CERT_SECRET_NAME = "my-cluster-broker-0";
    private static final Map<String, String> MTLS_CONFIG = Map.of(
            "namespace", NAMESPACE,
            "sslTrustStoreSecretName", CA_CERT_SECRET_NAME,
            "sslKeyStoreSecretName", NODE_CERT_SECRET_NAME);
    private static final Map<String, String> TLS_CONFIG = Map.of(
            "namespace", NAMESPACE,
            "sslKeyStoreSecretName", NODE_CERT_SECRET_NAME);

    @TempDir
    Path tempDir;

    private Server server;
    private HttpClient httpsClient;
    private HttpRequest httpsReq;
    private HttpRequest httpReq;
    private Secret caCertSecret;
    private Secret nodeCertSecret;
    private KubernetesClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws URISyntaxException, GeneralSecurityException, IOException {
        // self-signed cert with 100 years validity
        caCertSecret = new SecretBuilder()
                .withNewMetadata().withName(CA_CERT_SECRET_NAME).endMetadata()
                .withData(Map.of("ca.crt", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURjekNDQWx1Z0F3SUJBZ0lVSHQxQW9KN1JNL0dPNVNycm1EWGtkTzVUSlFvd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1NERUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeEVEQU9CZ05WQkFvTQpCMU4wY21sdGVta3hFakFRQmdOVkJBTU1DV3h2WTJGc2FHOXpkREFnRncweU5URXdNRFl4TVRBMU5EZGFHQTh5Ck1USTFNVEF3TnpFeE1EVTBOMW93U0RFTE1Ba0dBMVVFQmhNQ1FWVXhFekFSQmdOVkJBZ01DbE52YldVdFUzUmgKZEdVeEVEQU9CZ05WQkFvTUIxTjBjbWx0ZW1reEVqQVFCZ05WQkFNTUNXeHZZMkZzYUc5emREQ0NBU0l3RFFZSgpLb1pJaHZjTkFRRUJCUUFEZ2dFUEFEQ0NBUW9DZ2dFQkFOL0NVVG8vaTFOTElUdEZZeDBkbWtYVit6emdxSXBDCk1VaEFacnk3NTN4TzJiczFhWkhMa29zcnRvY2R3blBMUmhISk1qQzN4Zlp5MVAwcGN5a2RoUW5mRy9kNWZsWjYKdHZGOFRJT1VkK04vNGFsUUMrSnA3WUNyeTdmcE5yVFJPTDBlMVZhbnlzT0VVblNhYnZjU1VyL0NjcnF2MEwxTgppYnVoTGdVbERZTnBJcjhVMU03ZUwvQVR6aWpYQkpMR0ovb3pVeDRqVkJEWk9XM3ZWWXpwMWgvdVcwRTZEMGNnCnRqbnAvZDBlbHlFKzJ4L1JqRWxZYmZ4Q0ZFY0p2NGdqVkRtYk5mNklDTjN3Mkc2dGhXVFhhZ3ZwUGxrNXBLWS8KeEhsMzdGemxzb1NWdDFnbzlVKzZLUDMvV0tsS2JHZ3VyamJTSjhHR1pvWE9JQ1ZRQTdETTBpY0NBd0VBQWFOVApNRkV3SFFZRFZSME9CQllFRkhzRU01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1COEdBMVVkSXdRWU1CYUFGSHNFCk01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1BOEdBMVVkRXdFQi93UUZNQU1CQWY4d0RRWUpLb1pJaHZjTkFRRUwKQlFBRGdnRUJBSGFXY1RScHBZZHMyc1FORUwzbVhSZ1FPSW1qZHpCZ1NmMC9ha1FCUFFCNTNMMDZJQzJBS2pIbApjaHJublhqMEZkTEFsT29rcFBBZ0VVTFN1Z1NMNXVWRW1jZy83QThoUCtUZjFZZk9USG5ZNWl2MTIxcjMrNXlyCjJ0OEE5OXRGT0xGK1MwZ0gzYjRvNFpLaCtJRmc0UW9jcVZRZURMZ0pVZ1M0MGZJemZkZUhjZGJyZWt2QkFqUkUKc09lclZTVzlpTzRxSStUK3RQYTVsVUh4SWtzWmJlTng4WXlndlhxMVNLdDFLV0lpSEFoSmVqSlkxYmtrS2hpVQpSWEtzMGNLbjNwU1lSM0NsemNseHhZVndZcUJIYWpnUEdnajVKcC8vbEdYd0NQWVhzQnlhRktuWGxvTWJ5K0Q5Cm91cXV4VGhBNHRvVE5USSsrSVNVQWgyLzhYNElzVTg9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"))
                .build();

        nodeCertSecret = new SecretBuilder()
                .withNewMetadata().withName(NODE_CERT_SECRET_NAME).endMetadata()
                .withData(Map.of("my-cluster-broker-0.crt", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURjekNDQWx1Z0F3SUJBZ0lVSHQxQW9KN1JNL0dPNVNycm1EWGtkTzVUSlFvd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1NERUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeEVEQU9CZ05WQkFvTQpCMU4wY21sdGVta3hFakFRQmdOVkJBTU1DV3h2WTJGc2FHOXpkREFnRncweU5URXdNRFl4TVRBMU5EZGFHQTh5Ck1USTFNVEF3TnpFeE1EVTBOMW93U0RFTE1Ba0dBMVVFQmhNQ1FWVXhFekFSQmdOVkJBZ01DbE52YldVdFUzUmgKZEdVeEVEQU9CZ05WQkFvTUIxTjBjbWx0ZW1reEVqQVFCZ05WQkFNTUNXeHZZMkZzYUc5emREQ0NBU0l3RFFZSgpLb1pJaHZjTkFRRUJCUUFEZ2dFUEFEQ0NBUW9DZ2dFQkFOL0NVVG8vaTFOTElUdEZZeDBkbWtYVit6emdxSXBDCk1VaEFacnk3NTN4TzJiczFhWkhMa29zcnRvY2R3blBMUmhISk1qQzN4Zlp5MVAwcGN5a2RoUW5mRy9kNWZsWjYKdHZGOFRJT1VkK04vNGFsUUMrSnA3WUNyeTdmcE5yVFJPTDBlMVZhbnlzT0VVblNhYnZjU1VyL0NjcnF2MEwxTgppYnVoTGdVbERZTnBJcjhVMU03ZUwvQVR6aWpYQkpMR0ovb3pVeDRqVkJEWk9XM3ZWWXpwMWgvdVcwRTZEMGNnCnRqbnAvZDBlbHlFKzJ4L1JqRWxZYmZ4Q0ZFY0p2NGdqVkRtYk5mNklDTjN3Mkc2dGhXVFhhZ3ZwUGxrNXBLWS8KeEhsMzdGemxzb1NWdDFnbzlVKzZLUDMvV0tsS2JHZ3VyamJTSjhHR1pvWE9JQ1ZRQTdETTBpY0NBd0VBQWFOVApNRkV3SFFZRFZSME9CQllFRkhzRU01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1COEdBMVVkSXdRWU1CYUFGSHNFCk01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1BOEdBMVVkRXdFQi93UUZNQU1CQWY4d0RRWUpLb1pJaHZjTkFRRUwKQlFBRGdnRUJBSGFXY1RScHBZZHMyc1FORUwzbVhSZ1FPSW1qZHpCZ1NmMC9ha1FCUFFCNTNMMDZJQzJBS2pIbApjaHJublhqMEZkTEFsT29rcFBBZ0VVTFN1Z1NMNXVWRW1jZy83QThoUCtUZjFZZk9USG5ZNWl2MTIxcjMrNXlyCjJ0OEE5OXRGT0xGK1MwZ0gzYjRvNFpLaCtJRmc0UW9jcVZRZURMZ0pVZ1M0MGZJemZkZUhjZGJyZWt2QkFqUkUKc09lclZTVzlpTzRxSStUK3RQYTVsVUh4SWtzWmJlTng4WXlndlhxMVNLdDFLV0lpSEFoSmVqSlkxYmtrS2hpVQpSWEtzMGNLbjNwU1lSM0NsemNseHhZVndZcUJIYWpnUEdnajVKcC8vbEdYd0NQWVhzQnlhRktuWGxvTWJ5K0Q5Cm91cXV4VGhBNHRvVE5USSsrSVNVQWgyLzhYNElzVTg9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K",
                        "my-cluster-broker-0.key", "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRRGZ3bEU2UDR0VFN5RTcKUldNZEhacEYxZnM4NEtpS1FqRklRR2E4dStkOFR0bTdOV21SeTVLTEs3YUhIY0p6eTBZUnlUSXd0OFgyY3RUOQpLWE1wSFlVSjN4djNlWDVXZXJieGZFeURsSGZqZitHcFVBdmlhZTJBcTh1MzZUYTAwVGk5SHRWV3A4ckRoRkowCm1tNzNFbEsvd25LNnI5QzlUWW03b1M0RkpRMkRhU0svRk5UTzNpL3dFODRvMXdTU3hpZjZNMU1lSTFRUTJUbHQKNzFXTTZkWWY3bHRCT2c5SElMWTU2ZjNkSHBjaFB0c2YwWXhKV0czOFFoUkhDYitJSTFRNW16WCtpQWpkOE5odQpyWVZrMTJvTDZUNVpPYVNtUDhSNWQreGM1YktFbGJkWUtQVlB1aWo5LzFpcFNteG9McTQyMGlmQmhtYUZ6aUFsClVBT3d6TkluQWdNQkFBRUNnZ0VBSEx2U0ozczMwOWlPMUwxLzBVelhDSnh4RGdFVzA0NVZlc21Uc29KNkRhQ28KaUIycDhyTE94NDFlVVVmZjRSNXczemJVSkUvQ2N2ZmpEdndrSnRyOHdHYnZWZk82NXJXM3lXeVBVaDd6Z3ZvNAppeFZWanJFbDhtM2RQcjdnSzJScDkxWHBkUjV6Uk1hT0QzTGN5TFhkMGg4bVp0VWpVVFBrVHllNXNQMGE3azFlCnA3NVVSR2oyamRsejVQSGFlOGhXL1BMb1MyaWE1MXpjZnN1Y01wcURsNnY1aitUQ1pxQkh3Wlg5b1NtemhVYnAKWk5EWUpCSy9FWi9ZaGFaS0c0WE4vNE1LVndjeEw2cU44SDNYejVJVTFvdTM3UWcvU2pmSEtMUjc3MjJ5WHp0UgpNSUxPVzBBeUFWclJUVUs4RnVvTXlJTEg4UEpnTXBiQUpiMjRwcVhadVFLQmdRRHpZUmFtR1F5SHo3aXB2Ym9wCkI3elBNYnBxNWpmeC8reFpVeXViMlluVGNmck1kaGRCWEtzUXRqRlN1MWVzYytQSFlBWFo4SFpRTGM5V3M1disKcFAvMnZySDE4V1pndUFIVS9CSk5KeFE3MTF3UHF3TjlONzJacmwvSjRhYzBEc3ltS0l2QkdkaFg1a0dnM0YybApja01TQldDN0s5eG1DcEFlamhOR2cvd3FhUUtCZ1FEclhNUTRFUi9ScnlyUnp4b2pKUnU2RGNaU0hMQUFRL0hpCk5YVitUbkx3czQwdVhBNktvTFZuS1BMMXJIb1g1OVUvMEdyY0RJUGRSdHFMcEx1T25GREtQa1VkVk1icngxYkMKUWppOENlUXo4MWpTZlJIVzNrK1c1RzhUNE1JSlIxbXczck0zVFNRTHlNM3lYVkowN29FSlo3VlY4T1ZSdnhmUgplODJ1Mmd6bUR3S0JnUUNaRkRjUHIrK3V1SnQ0d0NvSVJxS2VXN1BhS3dXRFJDcGZvSzFzTUc2OVBSSzNhWXVGCkJBbGcwSWZEZHF4VmZ1c0U2ME9pNmRrdzR5OW5aRDg0OG9WQXFINzhwNkp5TVNxTjBTS2R2bmUrajkyS3lWQy8KZ01EVG1kY0wvcytSTWNITXZQSHlPaFJXYlRCWVFtTHdmaWJyZmRCeXljcXRyL1VvRXNyUzdvODhDUUtCZ0QwcAp5MWdpb3hrem96WUkwdXNGTHJKbjkvekl0YmdyOEFUd0RZdDRTWWhoc0xPMmVwVHQ5SlpOWHU0WEYxZDFDTWJmCm01VjVyeDdtMWM1cVRjOWVzZVFNMEpzeHQ4djM3b1RtL3FWbkVLV3JmSTZlcis4ZHNLTXUwK3JmZ3EwMG5JdEoKSkZ1ZnNWbGFvcUowUEFSbElxVldEUnE3VW15dTh6cWVLTEppdWUxakFvR0JBSTNEWWNsTk1JNXI0djBqODd1agpIajhYN0RzWmI4Yy85UTg5dzUzTlVDaG5icldLbDVaZjc4eE9xY2MvRWV6S2Roc3VNUHJ2ME5mRW5NdGpoTzI0ClhudzMwY2V6ZHM4R3BIUzgrVUpyKzZFSkt4Y2czZEJEU1lpRjVHZGo0NnkvZy9Tb1VLRkFHbFRkTGdGUzhnUEUKKzZXL2FOaXNhSTNMUHIydFh2Z1B1UFF5Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K"))
                .build();

        client = mock(KubernetesClient.class);
        MixedOperation<Secret, SecretList, Resource<Secret>> secrets = mock(MixedOperation.class);
        NonNamespaceOperation<Secret, SecretList, Resource<Secret>> namespacedSecrets = mock(NonNamespaceOperation.class);
        Resource<Secret> caCertResource = mock(Resource.class);
        Resource<Secret> nodeCertResource = mock(Resource.class);
        when(client.secrets()).thenReturn(secrets);
        when(secrets.inNamespace(NAMESPACE)).thenReturn(namespacedSecrets);
        when(namespacedSecrets.withName(CA_CERT_SECRET_NAME)).thenReturn(caCertResource);
        when(namespacedSecrets.withName(NODE_CERT_SECRET_NAME)).thenReturn(nodeCertResource);
        when(caCertResource.get()).thenReturn(caCertSecret);
        when(nodeCertResource.get()).thenReturn(nodeCertSecret);

        httpsClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .sslContext(getClientSSLContext(caCertSecret, nodeCertSecret))
                .build();

        httpsReq = HttpRequest.newBuilder()
                .uri(new URI("https://localhost:8443/v1/broker-state/"))
                .GET()
                .build();

        httpReq = HttpRequest.newBuilder()
                .uri(new URI("http://localhost:8080/v1/ready/"))
                .GET()
                .build();
    }

    private SSLContext getClientSSLContext(Secret caCertSecret, Secret nodeCertSecret) throws GeneralSecurityException, IOException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(KafkaAgentUtils.trustStore(caCertSecret));

        KeyManagerFactory kmf = null;
        if (nodeCertSecret != null) {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(KafkaAgentUtils.keyStore(nodeCertSecret), null);
        }

        SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
        sslContext.init(kmf != null ? kmf.getKeyManagers() : null, tmf.getTrustManagers(), new SecureRandom());
        return sslContext;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testBrokerRunningState() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);
        KafkaAgent agent = new KafkaAgent(client, MTLS_CONFIG, brokerState, null, null);
        server = agent.startHttpServer();

        HttpResponse<String> response = httpsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));

        String expectedResponse = "{\"brokerState\":3}";
        assertThat(expectedResponse, is(response.body()));
    }

    @Test
    public void testBrokerRunningStateWithoutMTls() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);
        KafkaAgent agent = new KafkaAgent(client, TLS_CONFIG, brokerState, null, null);
        server = agent.startHttpServer();

        HttpClient tlsClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .sslContext(getClientSSLContext(caCertSecret, null))
                .build();
        HttpResponse<String> response = tlsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));
        assertThat(response.body(), is("{\"brokerState\":3}"));
    }

    @Test
    public void testBrokerRunningStateWithoutTls() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);
        Map<String, String> config = Map.of();
        KafkaAgent agent = new KafkaAgent(client, config, brokerState, null, null);
        server = agent.startHttpServer();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI("http://localhost:8443/v1/broker-state/"))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));
        assertThat(response.body(), is("{\"brokerState\":3}"));
    }

    @Test
    public void testBrokerRecoveryState() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 2);

        @SuppressWarnings({ "rawtypes" })
        final Gauge remainingLogs = mock(Gauge.class);
        when(remainingLogs.value()).thenReturn((byte) 10);

        @SuppressWarnings({ "rawtypes" })
        final Gauge remainingSegments = mock(Gauge.class);
        when(remainingSegments.value()).thenReturn((byte) 100);

        KafkaAgent agent = new KafkaAgent(client, MTLS_CONFIG, brokerState, remainingLogs, remainingSegments);
        server = agent.startHttpServer();

        HttpResponse<String> response = httpsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));

        String expectedResponse = "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}";
        assertThat(response.body(), is(expectedResponse));
    }

    @Test
    public void testBrokerMetricNotFound() throws Exception {
        KafkaAgent agent = new KafkaAgent(client, MTLS_CONFIG, null, null, null);
        server = agent.startHttpServer();

        HttpResponse<String> response = httpsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    public void testReadinessSuccess() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);

        KafkaAgent agent = new KafkaAgent(client, MTLS_CONFIG, brokerState, null, null);
        server = agent.startHttpServer();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    public void testReadinessFail() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 2);

        KafkaAgent agent = new KafkaAgent(client, MTLS_CONFIG, brokerState, null, null);
        server = agent.startHttpServer();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_SERVICE_UNAVAILABLE));
    }

    /**
     * Creates the Kafka Agent configuration with TLS encryption and with Service Account authentication enabled. The
     * JWKS endpoint points to an unused port, because these tests check only the requests which are rejected before
     * the token is validated. The Kafka Agent is expected to start anyway and keep retrying the download of the keys
     * in the background.
     *
     * @return  Map with the Kafka Agent configuration
     */
    private Map<String, String> serviceAccountAuthenticationConfig() throws IOException {
        int unusedPort;

        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }

        Path tokenPath = tempDir.resolve("token");
        Files.writeString(tokenPath, "my-kubernetes-token");

        Map<String, String> config = new HashMap<>(TLS_CONFIG);
        config.put("tokenIssuer", "https://kubernetes.default.svc.cluster.local");
        config.put("tokenJwksUri", "http://localhost:" + unusedPort + "/openid/v1/jwks");
        config.put("tokenAudience", "strimzi.io/kafka/my-namespace/my-cluster");
        config.put("tokenAllowedUsers", "system:serviceaccount:my-namespace:my-cluster-cluster-operator");
        config.put("tokenPath", tokenPath.toString());

        return config;
    }

    @Test
    public void testExternalConnectorRequiresTokenWithServiceAccountAuthentication() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);

        KafkaAgent agent = new KafkaAgent(client, serviceAccountAuthenticationConfig(), brokerState, null, null);
        server = agent.startHttpServer();

        HttpClient tlsClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .sslContext(getClientSSLContext(caCertSecret, null))
                .build();
        HttpResponse<String> response = tlsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_UNAUTHORIZED));
    }

    @Test
    public void testInternalConnectorDoesNotRequireTokenWithServiceAccountAuthentication() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);

        KafkaAgent agent = new KafkaAgent(client, serviceAccountAuthenticationConfig(), brokerState, null, null);
        server = agent.startHttpServer();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    public void testExternalConnectorDoesNotRequireTokenWithoutServiceAccountAuthentication() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);

        KafkaAgent agent = new KafkaAgent(client, TLS_CONFIG, brokerState, null, null);
        server = agent.startHttpServer();

        HttpClient tlsClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .sslContext(getClientSSLContext(caCertSecret, null))
                .build();
        HttpResponse<String> response = tlsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));
    }

    @Test
    public void testReadinessFailWithBrokerUnknownState() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 127);

        KafkaAgent agent = new KafkaAgent(client, MTLS_CONFIG, brokerState, null, null);
        server = agent.startHttpServer();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(HttpServletResponse.SC_SERVICE_UNAVAILABLE));
    }
}
