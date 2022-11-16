/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ControllerQueueTest {
    @Test
    public void testEnqueueingEnqueued() {
        MeterRegistry metricsRegistry = new SimpleMeterRegistry();
        MetricsProvider metrics = new MicrometerMetricsProvider(metricsRegistry);
        ControllerQueue q = new ControllerQueue(10, new ControllerMetricsHolder("kind", Labels.EMPTY, metrics));

        SimplifiedReconciliation r1 = new SimplifiedReconciliation("kind", "my-namespace", "my-name", "watch");
        SimplifiedReconciliation r2 = new SimplifiedReconciliation("kind", "my-namespace", "my-name", "timer");
        SimplifiedReconciliation r3 = new SimplifiedReconciliation("kind", "my-nymespace", "my-other-name", "watch");

        q.enqueue(r1);
        q.enqueue(r3);
        q.enqueue(r2);

        assertThat(q.queue.size(), is(2));
        assertThat(q.queue.contains(r1), is(true));
        assertThat(q.queue.contains(r3), is(true));

        // Test metric
        assertThat(metricsRegistry.get("strimzi.reconciliations.already.enqueued").tag("kind", "kind").tag("namespace", "my-namespace").counter().count(), is(1.0));
    }
}
