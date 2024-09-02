/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.strimzi.operator.common.MicrometerMetricsProvider;

import java.time.Duration;

/**
 * Adds fine grained timer to the MicrometerMetricsProvider.
 */
public class TopicOperatorMetricsProvider extends MicrometerMetricsProvider {
    /**
     * Constructor of the metrics provider.
     *
     * @param metrics   Meter registry
     */
    public TopicOperatorMetricsProvider(MeterRegistry metrics) {
        super(metrics);
    }

    /**
     * Creates new Timer type metric with fine grained histogram buckets.
     * This can be used to measure the duration of internal operations.
     *
     * @param name          Name of the metric
     * @param description   Description of the metric
     * @param tags          Tags used for the metric
     * @return              Timer metric
     */
    public Timer fineGrainedTimer(String name, String description, Tags tags) {
        metrics.config().meterFilter(new CustomTimerFilter(name, new double[]{
            Duration.ofMillis(10).toNanos(),
            Duration.ofMillis(20).toNanos(),
            Duration.ofMillis(50).toNanos(),
            Duration.ofMillis(100).toNanos(),
            Duration.ofMillis(500).toNanos(),
            Duration.ofMillis(1000).toNanos(),
            Duration.ofMillis(5000).toNanos()
        }));
        return buildTimer(name, description, tags);
    }

    private Timer buildTimer(String name, String description, Tags tags) {
        return Timer.builder(name)
            .description(description)
            .tags(tags)
            .register(metrics);
    }
}
