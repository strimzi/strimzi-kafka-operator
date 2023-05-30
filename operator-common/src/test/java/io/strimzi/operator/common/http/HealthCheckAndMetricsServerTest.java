/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.http;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HealthCheckAndMetricsServerTest {
    @Test
    public void testAllGood() throws IOException, InterruptedException, URISyntaxException {
        MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        metrics.counter("my-metric", "My test metric", Tags.empty()).increment();

        Liveness liveness = () -> true;
        Readiness readiness = () -> true;

        int port = TestUtils.getFreePort();

        HealthCheckAndMetricsServer server = new HealthCheckAndMetricsServer(port, liveness, readiness, metrics);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();

            // Liveness
            HttpRequest request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/healthy")).GET().build();
            sendAndExpect(client, request, 200, "{\"status\": \"ok\"}");

            // Readiness
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/ready")).GET().build();
            sendAndExpect(client, request, 200, "{\"status\": \"ok\"}");

            // Metrics
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/metrics")).GET().build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode(), is(200));
            assertThat(response.body(), containsString("my_metric_total 1.0"));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testAllBad() throws IOException, InterruptedException, URISyntaxException {
        MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        metrics.counter("my-metric", "My test metric", Tags.empty()).increment();

        Liveness liveness = () -> false;
        Readiness readiness = () -> false;

        int port = TestUtils.getFreePort();

        HealthCheckAndMetricsServer server = new HealthCheckAndMetricsServer(port, liveness, readiness, metrics);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();

            // Liveness
            HttpRequest request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/healthy")).GET().build();
            sendAndExpect(client, request, 500, "{\"status\": \"not-ok\"}");

            // Readiness
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/ready")).GET().build();
            sendAndExpect(client, request, 500, "{\"status\": \"not-ok\"}");
        } finally {
            server.stop();
        }
    }

    private static void sendAndExpect(HttpClient client, HttpRequest request, int expectedStatus, String expectedBody) throws IOException, InterruptedException {
        HttpResponse<String> response =  client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(expectedStatus));
        assertThat(response.body().trim(), is(expectedBody));
    }

    @Test
    public void testDead() throws IOException, InterruptedException, URISyntaxException {
        MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        metrics.counter("my-metric", "My test metric", Tags.empty()).increment();

        Liveness liveness = () -> false;
        Readiness readiness = () -> true;

        int port = TestUtils.getFreePort();

        HealthCheckAndMetricsServer server = new HealthCheckAndMetricsServer(port, liveness, readiness, metrics);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();

            // Liveness
            HttpRequest request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/healthy")).GET().build();
            sendAndExpect(client, request, 500, "{\"status\": \"not-ok\"}");

            // Readiness
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/ready")).GET().build();
            sendAndExpect(client, request, 200, "{\"status\": \"ok\"}");
        } finally {
            server.stop();
        }
    }

    @Test
    public void testUnready() throws IOException, InterruptedException, URISyntaxException {
        MeterRegistry metricsRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        metrics.counter("my-metric", "My test metric", Tags.empty()).increment();

        Liveness liveness = () -> true;
        Readiness readiness = () -> false;

        int port = TestUtils.getFreePort();

        HealthCheckAndMetricsServer server = new HealthCheckAndMetricsServer(port, liveness, readiness, metrics);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();

            // Liveness
            HttpRequest request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/healthy")).GET().build();
            sendAndExpect(client, request, 200, "{\"status\": \"ok\"}");

            // Readiness
            request = HttpRequest.newBuilder().uri(new URI("http://localhost:" + port + "/ready")).GET().build();
            sendAndExpect(client, request, 500, "{\"status\": \"not-ok\"}");
        } finally {
            server.stop();
        }
    }
}
