/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.tracing.agent;

import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed tracing initialization based on OpenTelemetry
 */
public class OpenTelemetryTracing implements Tracing {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryTracing.class);

    @Override
    public void initialize() {
        String serviceName = System.getenv("OTEL_SERVICE_NAME");
        if (serviceName != null) {
            LOGGER.info("Initializing OpenTelemetry tracing with service name {}", serviceName);
            System.setProperty("otel.metrics.exporter", "none"); // disable otel metrics exporter. This has no effect on metrics in Strimzi.
            System.setProperty("otel.logs.exporter", "none"); // disable otel logs exporter. This has no effect on logging in Strimzi.
            AutoConfiguredOpenTelemetrySdk.initialize();
        } else {
            LOGGER.error("OpenTelemetry tracing cannot be initialized because OTEL_SERVICE_NAME environment variable is not defined");
        }
    }
}
