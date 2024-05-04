/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.core.Gauge;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAgentTest {
    private Server server;
    private ContextHandler context;
    private HttpRequest req;

    @BeforeEach
    public void setUp() throws URISyntaxException {
        server = new Server();
        ServerConnector conn = new ServerConnector(server);
        conn.setPort(8080);
        server.setConnectors(new Connector[] {conn});
        context = new ContextHandler("/");

        req = HttpRequest.newBuilder()
                .uri(new URI("http://localhost:8080/"))
                .GET()
                .build();
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
        KafkaAgent agent = new KafkaAgent(brokerState, null, null, null);
        context.setHandler(agent.getBrokerStateHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
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

        KafkaAgent agent = new KafkaAgent(brokerState, remainingLogs, remainingSegments, null);
        context.setHandler(agent.getBrokerStateHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_OK, is(response.statusCode()));

        String expectedResponse = "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}";
        assertThat(expectedResponse, is(response.body()));
    }

    @Test
    public void testBrokerMetricNotFound() throws Exception {
        KafkaAgent agent = new KafkaAgent(null, null, null, null);
        context.setHandler(agent.getBrokerStateHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_NOT_FOUND, is(response.statusCode()));

    }

    @Test
    public void testReadinessSuccess() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);

        KafkaAgent agent = new KafkaAgent(brokerState, null, null, null);
        context.setHandler(agent.getReadinessHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(HttpServletResponse.SC_NO_CONTENT, is(response.statusCode()));
    }

    @Test
    public void testReadinessFail() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 2);

        KafkaAgent agent = new KafkaAgent(brokerState, null, null, null);
        context.setHandler(agent.getReadinessHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(HttpServletResponse.SC_SERVICE_UNAVAILABLE, is(response.statusCode()));

    }

    @Test
    public void testReadinessFailWithBrokerUnknownState() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 127);

        KafkaAgent agent = new KafkaAgent(brokerState, null, null, null);
        context.setHandler(agent.getReadinessHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(HttpServletResponse.SC_SERVICE_UNAVAILABLE, is(response.statusCode()));

    }

    @Test
    public void testZkMigrationDone() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge zkMigrationState = mock(Gauge.class);
        when(zkMigrationState.value()).thenReturn(1);

        KafkaAgent agent = new KafkaAgent(null, null, null, zkMigrationState);
        context.setHandler(agent.getKRaftMigrationHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_OK, is(response.statusCode()));

        String expectedResponse = "{\"state\":1}";
        assertThat(expectedResponse, is(response.body()));
    }

    @Test
    public void testZkMigrationRunning() throws Exception {
        @SuppressWarnings({ "rawtypes" })
        final Gauge zkMigrationState = mock(Gauge.class);
        when(zkMigrationState.value()).thenReturn(2);

        KafkaAgent agent = new KafkaAgent(null, null, null, zkMigrationState);
        context.setHandler(agent.getKRaftMigrationHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_OK, is(response.statusCode()));

        String expectedResponse = "{\"state\":2}";
        assertThat(expectedResponse, is(response.body()));
    }

    @Test
    public void testZkMigrationMetricNotFound() throws Exception {
        KafkaAgent agent = new KafkaAgent(null, null, null, null);
        context.setHandler(agent.getKRaftMigrationHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(HttpServletResponse.SC_NOT_FOUND, is(response.statusCode()));
    }
}
