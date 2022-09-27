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
 * Distributed tracing initialization based on Jaeger/OpenTracing
 */
public class JaegerTracing implements Tracing {

    private static final Logger LOGGER = LoggerFactory.getLogger(JaegerTracing.class);

    @Override
    public void initialize() {
        String serviceName = System.getenv("JAEGER_SERVICE_NAME");
        if (serviceName != null) {
            LOGGER.info("Initializing Jaeger tracing with service name {}", serviceName);
            Tracer tracer = Configuration.fromEnv().getTracer();
            GlobalTracer.registerIfAbsent(tracer);
        } else {
            LOGGER.error("Jaeger tracing cannot be initialized because JAEGER_SERVICE_NAME environment variable is not defined");
        }
    }
}
