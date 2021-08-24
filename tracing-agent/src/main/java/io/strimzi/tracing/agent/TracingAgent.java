/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.tracing.agent;

import io.jaegertracing.Configuration;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very simple Java agent which initializes the Jaeger Tracer
 */
public class TracingAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(TracingAgent.class);

    /**
     * Agent entry point
     *
     * @param agentArgs The type of tracing which should be initialized
     */
    public static void premain(String agentArgs) {
        if (agentArgs != null && (agentArgs.equals("jaeger") || agentArgs.equals("opentelemetry"))) {
            boolean isOpenTelemetry = "opentelemetry".equals(agentArgs);

            String envName = isOpenTelemetry ? "OTEL_SERVICE_NAME" : "JAEGER_SERVICE_NAME";
            String serviceName = System.getenv(envName);

            if (serviceName != null) {
                LOGGER.info(
                    "Initializing {} tracing with service name {}",
                    isOpenTelemetry ? "OpenTelemetry" : "OpenTracing",
                    serviceName
                );

                if (isOpenTelemetry) {
                    AutoConfiguredOpenTelemetrySdk.initialize();
                } else {
                    Tracer tracer = Configuration.fromEnv().getTracer();
                    GlobalTracer.registerIfAbsent(tracer);
                }
            } else {
                LOGGER.error("Tracing cannot be initialized because the {} environment variable is not defined", envName);
            }
        }
    }
}