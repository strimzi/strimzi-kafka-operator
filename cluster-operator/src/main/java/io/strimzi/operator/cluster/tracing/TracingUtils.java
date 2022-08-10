/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.tracing;

import io.strimzi.api.kafka.model.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.operator.cluster.model.AbstractConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

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

    private static void addConfigOption(GetterSetter config, String key, String value) {
        Object previous = config.apply(key);
        if (previous != null) {
            config.accept(key, previous + "," + value);
        } else {
            config.accept(key, value);
        }
    }

    public interface GetterSetter extends Function<String, Object>, BiConsumer<String, Object> {
    }

    public static class AbstractConfigurationGetterSetter implements GetterSetter {
        private final AbstractConfiguration configuration;

        public AbstractConfigurationGetterSetter(AbstractConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public void accept(String key, Object value) {
            configuration.setConfigOption(key, value.toString());
        }

        @Override
        public String apply(String key) {
            return configuration.getConfigOption(key);
        }
    }

    public static class MapGetterSetter implements GetterSetter {
        private final Map<String, Object> configuration;

        public MapGetterSetter(Map<String, Object> configuration) {
            this.configuration = configuration;
        }

        @Override
        public void accept(String key, Object value) {
            configuration.put(key, value);
        }

        @Override
        public String apply(String key) {
            return (String) configuration.get(key);
        }
    }

    public static void addConsumerInterceptorClassName(Tracing tracing, GetterSetter config, String key) {
        addConfigOption(config, key, getTracingHandle(tracing).consumerInterceptorClassName());
    }

    public static void addProducerInterceptorClassName(Tracing tracing, GetterSetter config, String key) {
        addConfigOption(config, key, getTracingHandle(tracing).producerInterceptorClassName());
    }

    interface TracingHandle {
        String consumerInterceptorClassName();
        String producerInterceptorClassName();
    }

    static class OpenTracingHandle implements TracingHandle {
        @Override
        public String consumerInterceptorClassName() {
            return "io.opentracing.contrib.kafka.TracingConsumerInterceptor";
        }

        @Override
        public String producerInterceptorClassName() {
            return "io.opentracing.contrib.kafka.TracingProducerInterceptor";
        }
    }

    static class OTelTracingHandle implements TracingHandle {
        @Override
        public String consumerInterceptorClassName() {
            return "io.opentelemetry.instrumentation.kafkaclients.TracingConsumerInterceptor";
        }

        @Override
        public String producerInterceptorClassName() {
            return "io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor";
        }
    }
}
