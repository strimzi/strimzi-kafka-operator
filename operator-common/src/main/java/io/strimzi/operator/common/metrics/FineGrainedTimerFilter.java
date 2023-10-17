/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

import java.time.Duration;

/**
 * Timer filter that can be used to set a fine grained histogram buckets.
 */
public class FineGrainedTimerFilter implements MeterFilter {
    private String name;

    /**
     * Construct the timer filter.
     * @param name Name of the metric.
     */
    public FineGrainedTimerFilter(String name) {
        this.name = name;
    }

    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        if (id.getName().equals(name)) {
            return DistributionStatisticConfig.builder()
                .serviceLevelObjectives(Duration.ofMillis(10).toNanos(),
                    Duration.ofMillis(20).toNanos(),
                    Duration.ofMillis(50).toNanos(),
                    Duration.ofMillis(100).toNanos(),
                    Duration.ofMillis(500).toNanos(),
                    Duration.ofMillis(1000).toNanos(),
                    Duration.ofMillis(5000).toNanos())
                .build()
                .merge(config);
        }
        return config;
    }
}
