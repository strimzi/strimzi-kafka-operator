/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.http;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.operator.common.MetricsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

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
        contextHandler.setAllowNullPathInfo(true);
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
    class HealthyHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
            response.setContentType("application/json");

            if (liveness.isAlive()) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println("{\"status\": \"ok\"}");
            } else {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.getWriter().println("{\"status\": \"not-ok\"}");
            }
            LOGGER.debug("Responding {} to GET /healthy", response.getStatus());
            baseRequest.setHandled(true);
        }
    }

    /**
     * Handler responsible for the readiness check
     */
    class ReadyHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
            response.setContentType("application/json");

            if (readiness.isReady()) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println("{\"status\": \"ok\"}");
            } else {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.getWriter().println("{\"status\": \"not-ok\"}");

            }
            LOGGER.debug("Responding {} to GET /ready", response.getStatus());
            baseRequest.setHandled(true);
        }
    }

    /**
     * Handler responsible for the metrics
     */
    class MetricsHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
            response.setContentType("text/plain");

            if (prometheusMeterRegistry != null) {
                response.setStatus(HttpServletResponse.SC_OK);
                prometheusMeterRegistry.scrape(response.getWriter());
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
                response.getWriter().println("Prometheus metrics are not enabled");
            }

            baseRequest.setHandled(true);
        }
    }
}
