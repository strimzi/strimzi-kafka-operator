/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.tracing;

import io.strimzi.api.kafka.model.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.tracing.Tracing;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Handle any tracing abstraction
 */
public class TracingUtils {

    private static final Map<Class<? extends Tracing>, TracingHandle> HANDLE_MAP;

    static {
        HANDLE_MAP = new HashMap<>();
        HANDLE_MAP.put(JaegerTracing.class, new OpenTracingHandle());
        HANDLE_MAP.put(OpenTelemetryTracing.class, new OTelTracingHandle());
    }

    private static TracingHandle getTracingHandle(Tracing tracing) {
        Objects.requireNonNull(
            tracing,
            "Tracing configuration instance must not be null - missing null check in CR handling?"
        );
        Class<? extends Tracing> clazz = tracing.getClass();
        TracingHandle handle = HANDLE_MAP.get(clazz);
        if (handle == null) {
            throw new IllegalArgumentException("No matching TracingHandle for type: " + clazz.getName());
        }
        return handle;
    }

    public static String consumerInterceptor(Tracing tracing) {
        return getTracingHandle(tracing).consumerInterceptor();
    }

    public static String producerInterceptor(Tracing tracing) {
        return getTracingHandle(tracing).producerInterceptor();
    }

    interface TracingHandle {
        String consumerInterceptor();
        String producerInterceptor();
    }

    static class OpenTracingHandle implements TracingHandle {
        @Override
        public String consumerInterceptor() {
            return "io.opentracing.contrib.kafka.TracingConsumerInterceptor";
        }

        @Override
        public String producerInterceptor() {
            return "io.opentracing.contrib.kafka.TracingProducerInterceptor";
        }
    }

    static class OTelTracingHandle implements TracingHandle {
        @Override
        public String consumerInterceptor() {
            return "io.opentelemetry.instrumentation.kafkaclients.TracingConsumerInterceptor";
        }

        @Override
        public String producerInterceptor() {
            return "io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor";
        }
    }
}
