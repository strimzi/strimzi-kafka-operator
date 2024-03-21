/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface for providing metrics or their mocks
 */
public interface MetricsProvider {
    /**
     * Returns the Micrometer MeterRegistry with all metrics
     *
     * @return  MeterRegistry
     */
    MeterRegistry meterRegistry();

    /**
     * Creates new Counter type metric
     *
     * @param name          Name of the metric
     * @param description   Description of the metric
     * @param tags          Tags used for the metric
     * @return              Counter metric
     */
    Counter counter(String name, String description, Tags tags);

    /**
     * Creates new Timer type metric
     *
     * @param name          Name of the metric
     * @param description   Description of the metric
     * @param tags          Tags used for the metric
     * @return              Timer metric
     */
    Timer timer(String name, String description, Tags tags);

    /**
     * Creates new Gauge type metric
     *
     * @param name          Name of the metric
     * @param description   Description of the metric
     * @param tags          Tags used for the metric
     * @return              AtomicInteger which represents the Gauge metric
     */
    AtomicInteger gauge(String name, String description, Tags tags);

    /**
     * Creates new Gauge type metric of type Long
     *
     * @param name          Name of the metric
     * @param description   Description of the metric
     * @param tags          Tags used for the metric
     * @return              AtomicLong which represents the Gauge metric
     */
    AtomicLong gaugeLong(String name, String description, Tags tags);
}
