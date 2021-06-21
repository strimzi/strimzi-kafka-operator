/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.AbstractWatchableStatusedResourceOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(VertxExtension.class)
@Group("strimzi")
@Version("v1")
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

        FooWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        FooOperator operator = new FooOperator(vertx, "TestResource", resourceOperator, metrics, null) {

            @Override
            protected Future<Foo.MockStatus> createOrUpdate(Reconciliation reconciliation, Foo resource) {
                return Future.succeededFuture();
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, Foo resource) {
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Foo.MockStatus createStatus() {
                return new Foo.MockStatus();
            }
        };

        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge().value(), is(1.0));

                    context.completeNow();
                })));
    }

    @Test
    public void testFailingReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        FooWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        FooOperator operator = new FooOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future<Foo.MockStatus> createOrUpdate(Reconciliation reconciliation, Foo resource) {
                return Future.failedFuture(new RuntimeException("Test error"));
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, Foo resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Foo.MockStatus createStatus() {
                return new Foo.MockStatus();
            }
        };

        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .tag("reason", "Test error")
                            .gauge().value(), is(0.0));

                    context.completeNow();
                })));
    }

    @Test
    public void testPauseReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        FooWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingPausedResource();

        FooOperator operator = new FooOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future<Foo.MockStatus> createOrUpdate(Reconciliation reconciliation, Foo resource) {
                return Future.succeededFuture();
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, Foo resource) {
                return new HashSet<>();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Foo.MockStatus createStatus() {
                return new Foo.MockStatus();
            }
        };

        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resources.paused").tag("kind", "TestResource").gauge().value(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge().value(), is(1.0));

                    context.completeNow();
                })));
    }

    @Test
    public void testFailingWithLockReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        FooWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        FooOperator operator = new FooOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future<Foo.MockStatus> createOrUpdate(Reconciliation reconciliation, Foo resource) {
                return Future.failedFuture(new UnableToAcquireLockException());
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, Foo resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Foo.MockStatus createStatus() {
                return new Foo.MockStatus();
            }
        };

        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(0L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));

                    context.completeNow();
                })));
    }

    @Test
    public void testDeleteCountsReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        FooWatchableStatusedResourceOperator resourceOperator = new FooWatchableStatusedResourceOperator(vertx, null, "TestResource") {
            @Override
            protected MixedOperation<Foo, Foo.FooList, Resource<Foo>> operation() {
                return null;
            }

            @Override
            public Foo get(String namespace, String name) {
                return null;
            }

            @Override
            public Future<Foo> updateStatusAsync(Reconciliation reconciliation, Foo resource) {
                return null;
            }
        };

        FooOperator operator = new FooOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future<Foo.MockStatus> createOrUpdate(Reconciliation reconciliation, Foo resource) {
                return null;
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, Foo resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return Future.succeededFuture(Boolean.TRUE);
            }

            @Override
            protected Foo.MockStatus createStatus() {
                return new Foo.MockStatus();
            }
        };

        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThrows(MeterNotFoundException.class, () -> registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                                                                             .tag("kind", "TestResource")
                                                                             .tag("name", "my-resource")
                                                                             .tag("resource-namespace", "my-namespace")
                                                                             .gauge());

                    context.completeNow();
                })));
    }

    @Test
    public void testReconcileAll(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        Set<NamespaceAndName> resources = new HashSet<>(3);
        resources.add(new NamespaceAndName("my-namespace", "avfc"));
        resources.add(new NamespaceAndName("my-namespace", "vtid"));
        resources.add(new NamespaceAndName("my-namespace", "utv"));

        FooWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResource();

        FooOperator operator = new FooOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future<Foo.MockStatus> createOrUpdate(Reconciliation reconciliation, Foo resource) {
                return Future.succeededFuture();
            }

            public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
                return Future.succeededFuture(resources);
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, Foo resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            Future<Void> updateStatus(Reconciliation reconciliation, Foo.MockStatus desiredStatus) {
                return Future.succeededFuture();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected  Foo.MockStatus createStatus() {
                return new Foo.MockStatus();
            }
        };

        Promise<Void> reconcileAllPromise = Promise.promise();
        operator.reconcileAll("test", "my-namespace", reconcileAllPromise);

        reconcileAllPromise.future().onComplete(context.succeeding(v -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.periodical").tag("kind", "TestResource").counter().count(), is(1.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(3.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(3.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(0.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(0.0));

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(3L));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            for (NamespaceAndName resource : resources) {
                assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "TestResource")
                        .tag("name", resource.getName())
                        .tag("resource-namespace", resource.getNamespace())
                        .gauge().value(), is(1.0));
            }

            context.completeNow();
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

        registry.forEachMeter(registry::remove);

        return metrics;
    }

    protected abstract static class MyResource<S, T> extends CustomResource<S, T> {
    }

    protected FooWatchableStatusedResourceOperator resourceOperatorWithExistingResource()    {
        return new FooWatchableStatusedResourceOperator(vertx, null, "TestResource") {

            @Override
            public Future<Foo> updateStatusAsync(Reconciliation reconciliation, Foo resource) {
                return null;
            }

            @Override
            protected MixedOperation<Foo, Foo.FooList, Resource<Foo>> operation() {
                return null;
            }

            @Override
            public Foo get(String namespace, String name) {
                return new Foo();
            }
        };
    }

    private FooWatchableStatusedResourceOperator resourceOperatorWithExistingPausedResource()    {
        return new FooWatchableStatusedResourceOperator(vertx, null, "TestResource") {

            @Override
            public Future<Foo> updateStatusAsync(Reconciliation reconciliation, Foo resource) {
                return Future.succeededFuture();
            }

            @Override
            protected MixedOperation<Foo, Foo.FooList, Resource<Foo>> operation() {
                return null;
            }

            @Override
            public Foo get(String namespace, String name) {
                ObjectMeta md = new ObjectMeta();
                md.setAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"));

                return new Foo().withMeta(md);
            }

            @Override
            public Future<Foo> getAsync(String namespace, String name) {
                return Future.succeededFuture(get(namespace, name));
            }
        };
    }

    @Group("strimzi")
    @Version("v1")
    static class Foo extends MyResource<Foo.MockSpec, Foo.MockStatus> {

        ObjectMeta meta = new ObjectMeta();

        static class MockSpec extends Spec {
        }

        static class MockStatus extends Status {
        }

        static class FooList implements KubernetesResourceList<Foo> {

            @Override
            public ListMeta getMetadata() {
                return null;
            }

            @Override
            public List<Foo> getItems() {
                return null;
            }
        }

        @Override
        public ObjectMeta getMetadata() {
            return this.meta;
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
        public MockSpec getSpec() {
            return new MockSpec();
        }

        @Override
        public MockStatus getStatus() {
            return new MockStatus();
        }

        public Foo withMeta(ObjectMeta meta) {
            this.meta = meta;
            return this;
        }
    }

    abstract static class FooWatchableStatusedResourceOperator extends AbstractWatchableStatusedResourceOperator<KubernetesClient, Foo, Foo.FooList, Resource<Foo>> {
        public FooWatchableStatusedResourceOperator(Vertx vertx, KubernetesClient client, String resourceKind) {
            super(vertx, client, resourceKind);
        }
    }

    abstract static class FooOperator extends AbstractOperator<Foo, Foo.MockSpec, Foo.MockStatus, FooWatchableStatusedResourceOperator> {
        public FooOperator(Vertx vertx, String kind, FooWatchableStatusedResourceOperator resourceOperator, MetricsProvider metrics, Labels selectorLabels) {
            super(vertx, kind, resourceOperator, metrics, selectorLabels);
        }
    }
}
