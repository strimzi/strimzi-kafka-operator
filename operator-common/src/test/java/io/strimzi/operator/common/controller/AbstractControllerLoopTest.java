/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AbstractControllerLoopTest {
    private static final String KIND = "TestResource";
    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-resource";

    private static ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public static void beforeAll() {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterAll
    public static void afterAll() {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testInProgressStartTimeMetric() throws InterruptedException {
        ControllerMetricsHolder metrics = new ControllerMetricsHolder(KIND, null, new MicrometerMetricsProvider(new SimpleMeterRegistry()));
        MeterRegistry registry = metrics.metricsProvider().meterRegistry();

        CountDownLatch reconciliationStarted = new CountDownLatch(1);
        CountDownLatch reconciliationBlocked = new CountDownLatch(1);

        TestControllerLoop loop = new TestControllerLoop(new ControllerQueue(10, metrics), new ReconciliationLockManager(), metrics, reconciliationStarted, reconciliationBlocked);
        loop.start();

        try {
            long beforeReconcile = Instant.now().getEpochSecond();
            loop.enqueue(new SimplifiedReconciliation(KIND, NAMESPACE, NAME));

            assertThat(reconciliationStarted.await(10_000, TimeUnit.MILLISECONDS), is(true));

            double startTime = registry.get(MetricsHolder.METRICS_RECONCILIATIONS_IN_PROGRESS_START_TIME)
                    .tag("kind", KIND)
                    .tag("namespace", NAMESPACE)
                    .tag("name", NAME)
                    .gauge().value();
            assertThat(startTime, greaterThanOrEqualTo((double) beforeReconcile));
            assertThat(startTime, lessThanOrEqualTo((double) Instant.now().getEpochSecond()));

            reconciliationBlocked.countDown();

            TestUtils.waitFor("in-progress start time metric to be removed", 100, 10_000,
                    () -> registry.find(MetricsHolder.METRICS_RECONCILIATIONS_IN_PROGRESS_START_TIME).gauges().isEmpty());
        } finally {
            reconciliationBlocked.countDown();
            loop.stop();
        }
    }

    @Test
    public void testInProgressStartTimeMetricWhenLockNotAcquired() throws InterruptedException {
        ControllerMetricsHolder metrics = new ControllerMetricsHolder(KIND, null, new MicrometerMetricsProvider(new SimpleMeterRegistry()));
        MeterRegistry registry = metrics.metricsProvider().meterRegistry();
        ReconciliationLockManager lockManager = new ReconciliationLockManager();

        CountDownLatch reconciliationStarted = new CountDownLatch(1);
        CountDownLatch reconciliationBlocked = new CountDownLatch(1);

        TestControllerLoop loop = new TestControllerLoop(new ControllerQueue(10, metrics), lockManager, metrics, reconciliationStarted, reconciliationBlocked);
        loop.start();

        SimplifiedReconciliation reconciliation = new SimplifiedReconciliation(KIND, NAMESPACE, NAME);
        String lockName = reconciliation.lockName();

        // The test holds the lock, so the controller loop cannot acquire it and re-queues the reconciliation without registering the metric
        assertThat(lockManager.tryLock(lockName, 1_000, TimeUnit.MILLISECONDS), is(true));

        try {
            loop.enqueue(reconciliation);

            TestUtils.waitFor("reconciliation to fail to acquire the lock", 100, 10_000,
                    () -> metrics.lockedReconciliationsCounter(NAMESPACE).count() >= 1.0);

            assertThat(reconciliationStarted.getCount(), is(1L));
            assertThat(registry.find(MetricsHolder.METRICS_RECONCILIATIONS_IN_PROGRESS_START_TIME).gauges().isEmpty(), is(true));
        } finally {
            lockManager.unlock(lockName);
        }

        try {
            // Once the lock is released, the re-queued reconciliation runs and registers the metric
            assertThat(reconciliationStarted.await(10_000, TimeUnit.MILLISECONDS), is(true));
            assertThat(registry.find(MetricsHolder.METRICS_RECONCILIATIONS_IN_PROGRESS_START_TIME).gauges().isEmpty(), is(false));

            reconciliationBlocked.countDown();

            TestUtils.waitFor("in-progress start time metric to be removed", 100, 10_000,
                    () -> registry.find(MetricsHolder.METRICS_RECONCILIATIONS_IN_PROGRESS_START_TIME).gauges().isEmpty());
        } finally {
            reconciliationBlocked.countDown();
            loop.stop();
        }
    }

    /**
     * Controller loop which blocks inside the reconciliation until it is released by the test
     */
    static class TestControllerLoop extends AbstractControllerLoop {
        private final ControllerQueue workQueue;
        private final ControllerMetricsHolder metrics;
        private final CountDownLatch reconciliationStarted;
        private final CountDownLatch reconciliationBlocked;

        public TestControllerLoop(ControllerQueue workQueue, ReconciliationLockManager lockManager, ControllerMetricsHolder metrics, CountDownLatch reconciliationStarted, CountDownLatch reconciliationBlocked) {
            super("test-controller-loop", workQueue, lockManager, scheduledExecutor);
            this.workQueue = workQueue;
            this.metrics = metrics;
            this.reconciliationStarted = reconciliationStarted;
            this.reconciliationBlocked = reconciliationBlocked;
        }

        public void enqueue(SimplifiedReconciliation reconciliation) {
            workQueue.enqueue(reconciliation);
        }

        @Override
        protected void reconcile(Reconciliation reconciliation) {
            reconciliationStarted.countDown();
            try {
                reconciliationBlocked.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        protected ControllerMetricsHolder metrics() {
            return metrics;
        }
    }
}
