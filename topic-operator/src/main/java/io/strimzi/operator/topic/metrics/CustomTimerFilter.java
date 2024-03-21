/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

/**
 * Custom timer filter that can be used to get custom histogram buckets
 * by overriding the configuration of the meter with the given name.
 */
public class CustomTimerFilter implements MeterFilter {
    private String name;
    private double[] sla;

    /**
     * Construct the timer filter.
     * @param name Name of the metric.
     * @param sla Service level agreement (set of SLOs).
     */
    public CustomTimerFilter(String name, double[] sla) {
        this.name = name;
        this.sla = sla;
    }

    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        if (id.getName().equals(name)) {
            return DistributionStatisticConfig.builder()
                .serviceLevelObjectives(sla)
                .build()
                .merge(config);
        }
        return config;
    }
}
