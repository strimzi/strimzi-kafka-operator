/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.core.Gauge;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static io.strimzi.kafka.agent.KafkaAgent.RANDOM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAgentTest {
    private Server server;
    private ContextHandler context;
    private HttpClient httpsClient;
    private HttpRequest httpsReq;
    private HttpRequest httpReq;
    private Secret caCertSecret;
    private Secret nodeCertSecret;

    @BeforeEach
    public void setUp() throws URISyntaxException, GeneralSecurityException, IOException {
        // self-signed cert with 100 years validity
        caCertSecret = new SecretBuilder()
                .withNewMetadata().withName("my-cluster-ca-cert").endMetadata()
                .withData(Map.of("ca.crt", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURjekNDQWx1Z0F3SUJBZ0lVSHQxQW9KN1JNL0dPNVNycm1EWGtkTzVUSlFvd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1NERUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeEVEQU9CZ05WQkFvTQpCMU4wY21sdGVta3hFakFRQmdOVkJBTU1DV3h2WTJGc2FHOXpkREFnRncweU5URXdNRFl4TVRBMU5EZGFHQTh5Ck1USTFNVEF3TnpFeE1EVTBOMW93U0RFTE1Ba0dBMVVFQmhNQ1FWVXhFekFSQmdOVkJBZ01DbE52YldVdFUzUmgKZEdVeEVEQU9CZ05WQkFvTUIxTjBjbWx0ZW1reEVqQVFCZ05WQkFNTUNXeHZZMkZzYUc5emREQ0NBU0l3RFFZSgpLb1pJaHZjTkFRRUJCUUFEZ2dFUEFEQ0NBUW9DZ2dFQkFOL0NVVG8vaTFOTElUdEZZeDBkbWtYVit6emdxSXBDCk1VaEFacnk3NTN4TzJiczFhWkhMa29zcnRvY2R3blBMUmhISk1qQzN4Zlp5MVAwcGN5a2RoUW5mRy9kNWZsWjYKdHZGOFRJT1VkK04vNGFsUUMrSnA3WUNyeTdmcE5yVFJPTDBlMVZhbnlzT0VVblNhYnZjU1VyL0NjcnF2MEwxTgppYnVoTGdVbERZTnBJcjhVMU03ZUwvQVR6aWpYQkpMR0ovb3pVeDRqVkJEWk9XM3ZWWXpwMWgvdVcwRTZEMGNnCnRqbnAvZDBlbHlFKzJ4L1JqRWxZYmZ4Q0ZFY0p2NGdqVkRtYk5mNklDTjN3Mkc2dGhXVFhhZ3ZwUGxrNXBLWS8KeEhsMzdGemxzb1NWdDFnbzlVKzZLUDMvV0tsS2JHZ3VyamJTSjhHR1pvWE9JQ1ZRQTdETTBpY0NBd0VBQWFOVApNRkV3SFFZRFZSME9CQllFRkhzRU01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1COEdBMVVkSXdRWU1CYUFGSHNFCk01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1BOEdBMVVkRXdFQi93UUZNQU1CQWY4d0RRWUpLb1pJaHZjTkFRRUwKQlFBRGdnRUJBSGFXY1RScHBZZHMyc1FORUwzbVhSZ1FPSW1qZHpCZ1NmMC9ha1FCUFFCNTNMMDZJQzJBS2pIbApjaHJublhqMEZkTEFsT29rcFBBZ0VVTFN1Z1NMNXVWRW1jZy83QThoUCtUZjFZZk9USG5ZNWl2MTIxcjMrNXlyCjJ0OEE5OXRGT0xGK1MwZ0gzYjRvNFpLaCtJRmc0UW9jcVZRZURMZ0pVZ1M0MGZJemZkZUhjZGJyZWt2QkFqUkUKc09lclZTVzlpTzRxSStUK3RQYTVsVUh4SWtzWmJlTng4WXlndlhxMVNLdDFLV0lpSEFoSmVqSlkxYmtrS2hpVQpSWEtzMGNLbjNwU1lSM0NsemNseHhZVndZcUJIYWpnUEdnajVKcC8vbEdYd0NQWVhzQnlhRktuWGxvTWJ5K0Q5Cm91cXV4VGhBNHRvVE5USSsrSVNVQWgyLzhYNElzVTg9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"))
                .build();

        nodeCertSecret = new SecretBuilder()
                .withNewMetadata().withName("my-cluster-broker-0").endMetadata()
                .withData(Map.of("my-cluster-broker-0.crt", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURjekNDQWx1Z0F3SUJBZ0lVSHQxQW9KN1JNL0dPNVNycm1EWGtkTzVUSlFvd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1NERUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeEVEQU9CZ05WQkFvTQpCMU4wY21sdGVta3hFakFRQmdOVkJBTU1DV3h2WTJGc2FHOXpkREFnRncweU5URXdNRFl4TVRBMU5EZGFHQTh5Ck1USTFNVEF3TnpFeE1EVTBOMW93U0RFTE1Ba0dBMVVFQmhNQ1FWVXhFekFSQmdOVkJBZ01DbE52YldVdFUzUmgKZEdVeEVEQU9CZ05WQkFvTUIxTjBjbWx0ZW1reEVqQVFCZ05WQkFNTUNXeHZZMkZzYUc5emREQ0NBU0l3RFFZSgpLb1pJaHZjTkFRRUJCUUFEZ2dFUEFEQ0NBUW9DZ2dFQkFOL0NVVG8vaTFOTElUdEZZeDBkbWtYVit6emdxSXBDCk1VaEFacnk3NTN4TzJiczFhWkhMa29zcnRvY2R3blBMUmhISk1qQzN4Zlp5MVAwcGN5a2RoUW5mRy9kNWZsWjYKdHZGOFRJT1VkK04vNGFsUUMrSnA3WUNyeTdmcE5yVFJPTDBlMVZhbnlzT0VVblNhYnZjU1VyL0NjcnF2MEwxTgppYnVoTGdVbERZTnBJcjhVMU03ZUwvQVR6aWpYQkpMR0ovb3pVeDRqVkJEWk9XM3ZWWXpwMWgvdVcwRTZEMGNnCnRqbnAvZDBlbHlFKzJ4L1JqRWxZYmZ4Q0ZFY0p2NGdqVkRtYk5mNklDTjN3Mkc2dGhXVFhhZ3ZwUGxrNXBLWS8KeEhsMzdGemxzb1NWdDFnbzlVKzZLUDMvV0tsS2JHZ3VyamJTSjhHR1pvWE9JQ1ZRQTdETTBpY0NBd0VBQWFOVApNRkV3SFFZRFZSME9CQllFRkhzRU01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1COEdBMVVkSXdRWU1CYUFGSHNFCk01eU1zQXFaM2dqOTV5SEl5Qy9iVFF2Uk1BOEdBMVVkRXdFQi93UUZNQU1CQWY4d0RRWUpLb1pJaHZjTkFRRUwKQlFBRGdnRUJBSGFXY1RScHBZZHMyc1FORUwzbVhSZ1FPSW1qZHpCZ1NmMC9ha1FCUFFCNTNMMDZJQzJBS2pIbApjaHJublhqMEZkTEFsT29rcFBBZ0VVTFN1Z1NMNXVWRW1jZy83QThoUCtUZjFZZk9USG5ZNWl2MTIxcjMrNXlyCjJ0OEE5OXRGT0xGK1MwZ0gzYjRvNFpLaCtJRmc0UW9jcVZRZURMZ0pVZ1M0MGZJemZkZUhjZGJyZWt2QkFqUkUKc09lclZTVzlpTzRxSStUK3RQYTVsVUh4SWtzWmJlTng4WXlndlhxMVNLdDFLV0lpSEFoSmVqSlkxYmtrS2hpVQpSWEtzMGNLbjNwU1lSM0NsemNseHhZVndZcUJIYWpnUEdnajVKcC8vbEdYd0NQWVhzQnlhRktuWGxvTWJ5K0Q5Cm91cXV4VGhBNHRvVE5USSsrSVNVQWgyLzhYNElzVTg9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K",
                        "my-cluster-broker-0.key", "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRRGZ3bEU2UDR0VFN5RTcKUldNZEhacEYxZnM4NEtpS1FqRklRR2E4dStkOFR0bTdOV21SeTVLTEs3YUhIY0p6eTBZUnlUSXd0OFgyY3RUOQpLWE1wSFlVSjN4djNlWDVXZXJieGZFeURsSGZqZitHcFVBdmlhZTJBcTh1MzZUYTAwVGk5SHRWV3A4ckRoRkowCm1tNzNFbEsvd25LNnI5QzlUWW03b1M0RkpRMkRhU0svRk5UTzNpL3dFODRvMXdTU3hpZjZNMU1lSTFRUTJUbHQKNzFXTTZkWWY3bHRCT2c5SElMWTU2ZjNkSHBjaFB0c2YwWXhKV0czOFFoUkhDYitJSTFRNW16WCtpQWpkOE5odQpyWVZrMTJvTDZUNVpPYVNtUDhSNWQreGM1YktFbGJkWUtQVlB1aWo5LzFpcFNteG9McTQyMGlmQmhtYUZ6aUFsClVBT3d6TkluQWdNQkFBRUNnZ0VBSEx2U0ozczMwOWlPMUwxLzBVelhDSnh4RGdFVzA0NVZlc21Uc29KNkRhQ28KaUIycDhyTE94NDFlVVVmZjRSNXczemJVSkUvQ2N2ZmpEdndrSnRyOHdHYnZWZk82NXJXM3lXeVBVaDd6Z3ZvNAppeFZWanJFbDhtM2RQcjdnSzJScDkxWHBkUjV6Uk1hT0QzTGN5TFhkMGg4bVp0VWpVVFBrVHllNXNQMGE3azFlCnA3NVVSR2oyamRsejVQSGFlOGhXL1BMb1MyaWE1MXpjZnN1Y01wcURsNnY1aitUQ1pxQkh3Wlg5b1NtemhVYnAKWk5EWUpCSy9FWi9ZaGFaS0c0WE4vNE1LVndjeEw2cU44SDNYejVJVTFvdTM3UWcvU2pmSEtMUjc3MjJ5WHp0UgpNSUxPVzBBeUFWclJUVUs4RnVvTXlJTEg4UEpnTXBiQUpiMjRwcVhadVFLQmdRRHpZUmFtR1F5SHo3aXB2Ym9wCkI3elBNYnBxNWpmeC8reFpVeXViMlluVGNmck1kaGRCWEtzUXRqRlN1MWVzYytQSFlBWFo4SFpRTGM5V3M1disKcFAvMnZySDE4V1pndUFIVS9CSk5KeFE3MTF3UHF3TjlONzJacmwvSjRhYzBEc3ltS0l2QkdkaFg1a0dnM0YybApja01TQldDN0s5eG1DcEFlamhOR2cvd3FhUUtCZ1FEclhNUTRFUi9ScnlyUnp4b2pKUnU2RGNaU0hMQUFRL0hpCk5YVitUbkx3czQwdVhBNktvTFZuS1BMMXJIb1g1OVUvMEdyY0RJUGRSdHFMcEx1T25GREtQa1VkVk1icngxYkMKUWppOENlUXo4MWpTZlJIVzNrK1c1RzhUNE1JSlIxbXczck0zVFNRTHlNM3lYVkowN29FSlo3VlY4T1ZSdnhmUgplODJ1Mmd6bUR3S0JnUUNaRkRjUHIrK3V1SnQ0d0NvSVJxS2VXN1BhS3dXRFJDcGZvSzFzTUc2OVBSSzNhWXVGCkJBbGcwSWZEZHF4VmZ1c0U2ME9pNmRrdzR5OW5aRDg0OG9WQXFINzhwNkp5TVNxTjBTS2R2bmUrajkyS3lWQy8KZ01EVG1kY0wvcytSTWNITXZQSHlPaFJXYlRCWVFtTHdmaWJyZmRCeXljcXRyL1VvRXNyUzdvODhDUUtCZ0QwcAp5MWdpb3hrem96WUkwdXNGTHJKbjkvekl0YmdyOEFUd0RZdDRTWWhoc0xPMmVwVHQ5SlpOWHU0WEYxZDFDTWJmCm01VjVyeDdtMWM1cVRjOWVzZVFNMEpzeHQ4djM3b1RtL3FWbkVLV3JmSTZlcis4ZHNLTXUwK3JmZ3EwMG5JdEoKSkZ1ZnNWbGFvcUowUEFSbElxVldEUnE3VW15dTh6cWVLTEppdWUxakFvR0JBSTNEWWNsTk1JNXI0djBqODd1agpIajhYN0RzWmI4Yy85UTg5dzUzTlVDaG5icldLbDVaZjc4eE9xY2MvRWV6S2Roc3VNUHJ2ME5mRW5NdGpoTzI0ClhudzMwY2V6ZHM4R3BIUzgrVUpyKzZFSkt4Y2czZEJEU1lpRjVHZGo0NnkvZy9Tb1VLRkFHbFRkTGdGUzhnUEUKKzZXL2FOaXNhSTNMUHIydFh2Z1B1UFF5Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K"))
                .build();

        server = new Server();

        ServerConnector httpsConn = new ServerConnector(server,
                new SslConnectionFactory(KafkaAgent.getSSLContextFactory(caCertSecret, nodeCertSecret), "http/1.1"),
                new HttpConnectionFactory(new HttpConfiguration()));
        httpsConn.setHost("localhost");
        httpsConn.setPort(8443);

        ServerConnector httpConn  = new ServerConnector(server);
        httpConn.setHost("localhost");
        httpConn.setPort(8080);

        server.setConnectors(new Connector[] {httpsConn, httpConn});
        context = new ContextHandler("/");

        httpsClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .sslContext(getClientSSLContext(caCertSecret, nodeCertSecret))
                .build();

        httpsReq = HttpRequest.newBuilder()
                .uri(new URI("https://localhost:8443/"))
                .GET()
                .build();

        httpReq = HttpRequest.newBuilder()
                .uri(new URI("http://localhost:8080/"))
                .GET()
                .build();
    }

    private SSLContext getClientSSLContext(Secret caCertSecret, Secret nodeCertSecret) throws GeneralSecurityException, IOException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(KafkaAgentUtils.jksTrustStore(caCertSecret));

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        byte[] random = new byte[24];
        RANDOM.nextBytes(random);
        String password = Base64.getUrlEncoder().withoutPadding().encodeToString(random).substring(0, 32);
        kmf.init(KafkaAgentUtils.jksKeyStore(nodeCertSecret, password.toCharArray()), password.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), RANDOM);
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
        KafkaAgent agent = new KafkaAgent(caCertSecret, nodeCertSecret, brokerState, null, null);
        context.setHandler(agent.getBrokerStateHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = httpsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));

        String expectedResponse = "{\"brokerState\":3}";
        assertThat(expectedResponse, is(response.body()));
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

        KafkaAgent agent = new KafkaAgent(caCertSecret, nodeCertSecret, brokerState, remainingLogs, remainingSegments);
        context.setHandler(agent.getBrokerStateHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = httpsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_OK, is(response.statusCode()));

        String expectedResponse = "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}";
        assertThat(expectedResponse, is(response.body()));
    }

    @Test
    public void testBrokerMetricNotFound() throws Exception {
        KafkaAgent agent = new KafkaAgent(caCertSecret, nodeCertSecret, null, null, null);
        context.setHandler(agent.getBrokerStateHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = httpsClient.send(httpsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_NOT_FOUND, is(response.statusCode()));
    }

    @Test
    public void testReadinessSuccess() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);

        KafkaAgent agent = new KafkaAgent(caCertSecret, nodeCertSecret, brokerState, null, null);
        context.setHandler(agent.getReadinessHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(HttpServletResponse.SC_NO_CONTENT, is(response.statusCode()));
    }

    @Test
    public void testReadinessFail() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 2);

        KafkaAgent agent = new KafkaAgent(caCertSecret, nodeCertSecret, brokerState, null, null);
        context.setHandler(agent.getReadinessHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(HttpServletResponse.SC_SERVICE_UNAVAILABLE, is(response.statusCode()));
    }

    @Test
    public void testReadinessFailWithBrokerUnknownState() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 127);

        KafkaAgent agent = new KafkaAgent(caCertSecret, nodeCertSecret, brokerState, null, null);
        context.setHandler(agent.getReadinessHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(httpReq, HttpResponse.BodyHandlers.ofString());

        assertThat(HttpServletResponse.SC_SERVICE_UNAVAILABLE, is(response.statusCode()));
    }
}
