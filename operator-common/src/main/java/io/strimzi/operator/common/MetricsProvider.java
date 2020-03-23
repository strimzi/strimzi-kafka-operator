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

/**
 * Interface to be implemented for returning an instance of Kafka Admin interface
 */
public interface MetricsProvider {
    MeterRegistry meterRegistry();

    Counter counter(String name, String description, Tags tags);

    Timer timer(String name, String description, Tags tags);

    AtomicInteger gauge(String name, String description, Tags tags);
}
