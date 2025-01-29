/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.http;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.operator.common.MetricsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.Callback;

import javax.servlet.http.HttpServletResponse;

import java.nio.charset.StandardCharsets;

/**
 * Jetty based web server used for health checks and metrics
 */
public class HealthCheckAndMetricsServer {
    private final static Logger LOGGER = LogManager.getLogger(HealthCheckAndMetricsServer.class);
    private static final int HEALTH_CHECK_PORT = 8081;

    private final Server server;
    private final Liveness liveness;

    private final Readiness readiness;
    private final PrometheusMeterRegistry prometheusMeterRegistry;

    /**
     * Constructs the health check and metrics webserver. This constructor will use the default port 8081.
     *
     * @param liveness          Callback used for the health check.
     * @param readiness         Callback used for the readiness check.
     * @param metricsProvider   Metrics provider for integrating Prometheus metrics.
     */
    public HealthCheckAndMetricsServer(Liveness liveness, Readiness readiness, MetricsProvider metricsProvider) {
        this(HEALTH_CHECK_PORT, liveness, readiness, metricsProvider);
    }

    /**
     * Constructs the health check and metrics webserver. This constructor has a configurable port and is designed to be
     * used in tests.
     *
     * @param port              Port number which should be used by the web server.
     * @param liveness          Callback used for the health check.
     * @param readiness         Callback used for the readiness check.
     * @param metricsProvider   Metrics provider for integrating Prometheus metrics.
     */
    public HealthCheckAndMetricsServer(int port, Liveness liveness, Readiness readiness, MetricsProvider metricsProvider) {
        this.liveness = liveness;
        this.readiness = readiness;
        // If the metrics provider is Prometheus based, we integrate it into the webserver
        this.prometheusMeterRegistry = metricsProvider != null && metricsProvider.meterRegistry() instanceof PrometheusMeterRegistry ? (PrometheusMeterRegistry) metricsProvider.meterRegistry() : null;

        // Set up the Jetty webserver
        server = new Server(port);

        // Configure Handlers
        ContextHandlerCollection contexts = new ContextHandlerCollection();

        contexts.addHandler(contextHandler("/metrics", new MetricsHandler()));

        if (liveness != null) {
            contexts.addHandler(contextHandler("/healthy", new HealthyHandler()));
        }

        if (readiness != null) {
            contexts.addHandler(contextHandler("/ready", new ReadyHandler()));
        }

        server.setHandler(contexts);
    }

    private static ContextHandler contextHandler(String path, Handler handler) {
        LOGGER.debug("Configuring path {} with handler {}", path, handler);
        ContextHandler contextHandler = new ContextHandler();
        contextHandler.setContextPath(path);
        contextHandler.setHandler(handler);
        contextHandler.setAllowNullPathInContext(true);
        return contextHandler;
    }

    /**
     * Starts the webserver
     */
    public void start() {
        try {
            server.start();
        } catch (Exception e)   {
            LOGGER.error("Failed to start the health check and metrics webserver", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Stops the webserver
     */
    public void stop() {
        try {
            server.stop();
        } catch (Exception e)   {
            LOGGER.error("Failed to stop the health check and metrics webserver", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Handler responsible for the liveness check
     */
    class HealthyHandler extends Handler.Abstract {
        @Override
        public boolean handle(Request request, Response response, Callback callback) throws Exception {
            response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json; charset=UTF-8");

            if (liveness.isAlive()) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.write(true, StandardCharsets.UTF_8.encode("{\"status\": \"ok\"}"), callback);
            } else {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.write(true, StandardCharsets.UTF_8.encode("{\"status\": \"not-ok\"}"), callback);
            }

            LOGGER.debug("Responding {} to GET /healthy", response.getStatus());

            return true;
        }
    }

    /**
     * Handler responsible for the readiness check
     */
    class ReadyHandler extends Handler.Abstract {
        @Override
        public boolean handle(Request request, Response response, Callback callback) throws Exception {
            response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json; charset=UTF-8");

            if (readiness.isReady()) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.write(true, StandardCharsets.UTF_8.encode("{\"status\": \"ok\"}"), callback);
            } else {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.write(true, StandardCharsets.UTF_8.encode("{\"status\": \"not-ok\"}"), callback);
            }

            LOGGER.debug("Responding {} to GET /healthy", response.getStatus());

            return true;
        }
    }

    /**
     * Handler responsible for the metrics
     */
    class MetricsHandler extends Handler.Abstract {
        @Override
        public boolean handle(Request request, Response response, Callback callback) throws Exception {
            if (prometheusMeterRegistry != null) {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "text/plain; version=0.0.4; charset=UTF-8");
                response.setStatus(HttpServletResponse.SC_OK);
                response.write(true, StandardCharsets.UTF_8.encode(prometheusMeterRegistry.scrape()), callback);
            } else {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "text/plain; charset=UTF-8");
                response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
                response.write(true, StandardCharsets.UTF_8.encode("Prometheus metrics are not enabled"), callback);
            }

            LOGGER.debug("Responding {} to GET /metrics", response.getStatus());

            return true;
        }
    }
}
