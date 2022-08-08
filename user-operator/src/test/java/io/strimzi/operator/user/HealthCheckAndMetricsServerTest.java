/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthCheckAndMetricsServerTest {
    @Test
    public void testAllGood() throws IOException, InterruptedException, URISyntaxException {
        MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        metrics.counter("my-metric", "My test metric", Tags.empty()).increment();

        UserController controller = mock(UserController.class);
        when(controller.isAlive()).thenReturn(true);
        when(controller.isReady()).thenReturn(true);

        int port = getFreePort();

        HealthCheckAndMetricsServer server = new HealthCheckAndMetricsServer(port, controller, metrics);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();

            // Liveness
            HttpRequest request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/healthy")).GET().build();
            HttpResponse<String> response =  client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode(), is(200));
            assertThat(response.body().trim(), is("{\"status\": \"ok\"}"));

            // Readiness
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/ready")).GET().build();
            response =  client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode(), is(200));
            assertThat(response.body().trim(), is("{\"status\": \"ok\"}"));

            // Metrics
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/metrics")).GET().build();
            response =  client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode(), is(200));
            assertThat(response.body(), containsString("my_metric_total 1.0"));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testNotGood() throws IOException, InterruptedException, URISyntaxException {
        MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        metrics.counter("my-metric", "My test metric", Tags.empty()).increment();

        UserController controller = mock(UserController.class);
        when(controller.isAlive()).thenReturn(false);
        when(controller.isReady()).thenReturn(false);

        int port = getFreePort();

        HealthCheckAndMetricsServer server = new HealthCheckAndMetricsServer(port, controller, metrics);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();

            // Liveness
            HttpRequest request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/healthy")).GET().build();
            HttpResponse<String> response =  client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode(), is(500));
            assertThat(response.body().trim(), is("{\"status\": \"not-ok\"}"));

            // Readiness
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/ready")).GET().build();
            response =  client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode(), is(500));
            assertThat(response.body().trim(), is("{\"status\": \"not-ok\"}"));
        } finally {
            server.stop();
        }
    }

    /**
     * Finds a free server port which can be used by the web server
     *
     * @return  A free TCP port
     */
    private int getFreePort()   {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find free port", e);
        }
    }
}
