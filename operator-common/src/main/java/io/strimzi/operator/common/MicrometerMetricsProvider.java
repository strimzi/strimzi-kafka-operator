/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.vertx.micrometer.backends.BackendRegistries;

import java.util.concurrent.atomic.AtomicInteger;

public class MicrometerMetricsProvider implements MetricsProvider {
    private final MeterRegistry metrics;

    public MicrometerMetricsProvider() {
        this.metrics = BackendRegistries.getDefaultNow();
    }

    @Override
    public MeterRegistry meterRegistry() {
        return metrics;
    }

    @Override
    public Counter counter(String name, String description, Tags tags) {
        return Counter.builder(name)
                .description(description)
                .tags(tags)
                .register(metrics);
    }

    @Override
    public Timer timer(String name, String description, Tags tags) {
        return Timer.builder(name)
                .description(description)
                .tags(tags)
                .register(metrics);
    }

    @Override
    public AtomicInteger gauge(String name, String description, Tags tags) {
        AtomicInteger gauge = new AtomicInteger(0);
        Gauge.builder(name, () -> gauge)
                .description(description)
                .tags(tags)
                .register(metrics);

        return gauge;
    }
}
