/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.tracing.agent;

import io.jaegertracing.Configuration;
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
        if ("jaeger".equals(agentArgs)) {
            String jaegerServiceName = System.getenv("JAEGER_SERVICE_NAME");

            if (jaegerServiceName != null) {
                LOGGER.info("Initializing Jaeger tracing with service name {}", jaegerServiceName);
                Tracer tracer = Configuration.fromEnv().getTracer();
                GlobalTracer.registerIfAbsent(tracer);
            } else {
                LOGGER.error("Jaeger tracing cannot be initialized because JAEGER_SERVICE_NAME environment variable is not defined");
            }
        }
    }
}