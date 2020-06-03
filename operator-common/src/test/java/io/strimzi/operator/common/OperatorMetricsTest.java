/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.AbstractWatchableResourceOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@ExtendWith(VertxExtension.class)
public class OperatorMetricsTest {
    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)
        ));
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testSuccessfulReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics) {
            @Override
            protected Future<Void> createOrUpdate(Reconciliation reconciliation, HasMetadata resource) {
                return Future.succeededFuture();
            }

            protected void validate(HasMetadata resource) {
                // Do nothing
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    async.flag();
                })));
    }

    @Test
    public void testFailingReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics) {
            @Override
            protected Future<Void> createOrUpdate(Reconciliation reconciliation, HasMetadata resource) {
                return Future.failedFuture(new RuntimeException("Test error"));
            }

            protected void validate(HasMetadata resource) {
                // Do nothing
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    async.flag();
                })));
    }

    @Test
    public void testFailingWithLockReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics) {
            @Override
            protected Future<Void> createOrUpdate(Reconciliation reconciliation, HasMetadata resource) {
                return Future.failedFuture(new UnableToAcquireLockException());
            }

            protected void validate(HasMetadata resource) {
                // Do nothing
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(0L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));

                    async.flag();
                })));
    }

    @Test
    public void testDeleteCountsReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableResourceOperator resourceOperator = new AbstractWatchableResourceOperator(vertx, null, "TestResource") {
            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public HasMetadata get(String namespace, String name) {
                return null;
            }
        };

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics) {
            @Override
            protected Future<Void> createOrUpdate(Reconciliation reconciliation, HasMetadata resource) {
                return null;
            }

            protected void validate(HasMetadata resource) {
                // Do nothing
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return Future.succeededFuture(Boolean.TRUE);
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    async.flag();
                })));
    }

    @Test
    public void testReconcileAll(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics) {
            @Override
            protected Future<Void> createOrUpdate(Reconciliation reconciliation, HasMetadata resource) {
                return Future.succeededFuture();
            }

            public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
                Set<NamespaceAndName> resources = new HashSet<>(3);
                resources.add(new NamespaceAndName("my-namespace", "avfc"));
                resources.add(new NamespaceAndName("my-namespace", "vtid"));
                resources.add(new NamespaceAndName("my-namespace", "utv"));

                return Future.succeededFuture(resources);
            }

            protected void validate(HasMetadata resource) {
                // Do nothing
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }
        };

        Promise<Void> reconcileAllPromise = Promise.promise();
        operator.reconcileAll("test", "my-namespace", reconcileAllPromise);

        Checkpoint async = context.checkpoint();
        reconcileAllPromise.future().onComplete(context.succeeding(v -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.periodical").tag("kind", "TestResource").counter().count(), is(1.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(3.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(3.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(3L));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            async.flag();
        })));
    }

    /**
     * Created new MetricsProvider and makes sure it doesn't contain any metrics from previous tests.
     *
     * @return  Clean MetricsProvider
     */
    public MetricsProvider createCleanMetricsProvider() {
        MetricsProvider metrics = new MicrometerMetricsProvider();
        MeterRegistry registry = metrics.meterRegistry();

        registry.forEachMeter(meter -> {
            registry.remove(meter);
        });

        return metrics;
    }

    private AbstractWatchableResourceOperator resourceOperatorWithExistingResource()    {
        return new AbstractWatchableResourceOperator(vertx, null, "TestResource") {
            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public HasMetadata get(String namespace, String name) {
                return new HasMetadata() {
                    @Override
                    public ObjectMeta getMetadata() {
                        return null;
                    }

                    @Override
                    public void setMetadata(ObjectMeta objectMeta) {

                    }

                    @Override
                    public String getKind() {
                        return "TestResource";
                    }

                    @Override
                    public String getApiVersion() {
                        return "v1";
                    }

                    @Override
                    public void setApiVersion(String s) {

                    }
                };
            }
        };
    }
}
