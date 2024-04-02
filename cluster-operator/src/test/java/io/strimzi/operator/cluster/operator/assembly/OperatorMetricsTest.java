/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractWatchableStatusedNamespacedResourceOperator;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(VertxExtension.class)
@Group("strimzi")
@Version("v1")
@SuppressWarnings({"unchecked", "rawtypes"})
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

    public void successfulReconcile(VertxTestContext context, Labels selectorLabels)  {
        MetricsProvider metricsProvider = createCleanMetricsProvider();

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithSelectorLabel(selectorLabels);

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metricsProvider, selectorLabels) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.succeededFuture();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus(CustomResource ignored) {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metricsProvider.meterRegistry();
                    Tag selectorTag = Tag.of("selector", selectorLabels != null ? selectorLabels.toSelectorString() : "");

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge().value(), is(1.0));

                    async.flag();
                })));
    }

    @Test
    public void testReconcileWithEmptySelectorLabels(VertxTestContext context) {
        successfulReconcile(context, Labels.fromMap(emptyMap()));
    }

    @Test
    public void testReconcileWithValuedSelectorLabel(VertxTestContext context) {
        successfulReconcile(context, Labels.fromMap(Collections.singletonMap("io/my-test-label", "my-test-value")));
    }

    @Test
    public void testReconcileWithoutSelectorLabel(VertxTestContext context) {
        successfulReconcile(context, null);
    }

    public void failingReconcile(VertxTestContext context, Labels selectorLabels)  {
        MetricsProvider metricsProvider = createCleanMetricsProvider();

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithSelectorLabel(selectorLabels);

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metricsProvider, selectorLabels) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.failedFuture(new RuntimeException("Test error"));
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus(CustomResource ignored) {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metricsProvider.meterRegistry();
                    Tag selectorTag = Tag.of("selector", selectorLabels != null ? selectorLabels.toSelectorString() : "");

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_FAILED).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_FAILED).tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .tag("reason", "Test error")
                            .gauge().value(), is(0.0));

                    async.flag();
                })));
    }

    @Test
    public void testFailingReconcileWithEmptySelectorLabels(VertxTestContext context) {
        failingReconcile(context, Labels.fromMap(emptyMap()));
    }

    @Test
    public void testFailingReconcileWithValuedSelectorLabel(VertxTestContext context) {
        failingReconcile(context, Labels.fromMap(Collections.singletonMap("io/my-test-label", "my-test-value")));
    }

    @Test
    public void testFailingReconcileWithoutSelectorLabel(VertxTestContext context) {
        failingReconcile(context, null);
    }

    @Test
    public void testPauseReconcile(VertxTestContext context)  {
        MetricsProvider metricsProvider = createCleanMetricsProvider();

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = resourceOperatorWithExistingPausedResource();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metricsProvider, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.succeededFuture();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus(CustomResource ignored) {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metricsProvider.meterRegistry();
                    Tag selectorTag = Tag.of("selector", "");

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCES_PAUSED).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCES_PAUSED).tag("kind", "TestResource").gauge().value(), is(1.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge().value(), is(1.0));

                    async.flag();
                })));
    }

    @Test
    public void testFailingWithLockReconcile(VertxTestContext context)  {
        MetricsProvider metricsProvider = createCleanMetricsProvider();

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithoutSelectorLabel();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metricsProvider, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.failedFuture(new UnableToAcquireLockException());
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus(CustomResource ignored) {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metricsProvider.meterRegistry();
                    Tag selectorTag = Tag.of("selector", "");

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_LOCKED).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_LOCKED).tag("kind", "TestResource").counter().count(), is(1.0));
                    async.flag();
                })));
    }

    @Test
    public void testDeleteCountsReconcile(VertxTestContext context)  {
        MetricsProvider metricsProvider = createCleanMetricsProvider();

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = new AbstractWatchableStatusedNamespacedResourceOperator(vertx, null, "TestResource") {
            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public HasMetadata get(String namespace, String name) {
                return null;
            }

            @Override
            public Future getAsync(String namespace, String name) {
                return Future.succeededFuture();
            }

            @Override
            public Future updateStatusAsync(Reconciliation reconciliation, HasMetadata resource) {
                return null;
            }
        };

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metricsProvider, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return null;
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return Future.succeededFuture(Boolean.TRUE);
            }

            @Override
            protected Status createStatus(CustomResource ignored) {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metricsProvider.meterRegistry();
                    Tag selectorTag = Tag.of("selector", "");

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThrows(MeterNotFoundException.class, () -> registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge());

                    async.flag();
                })));
    }

    @Test
    public void testReconcileAll(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        Set<NamespaceAndName> resources = new HashSet<>(3);
        resources.add(new NamespaceAndName("my-namespace", "avfc"));
        resources.add(new NamespaceAndName("my-namespace", "vtid"));
        resources.add(new NamespaceAndName("my-namespace", "utv"));

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithoutSelectorLabel();
        AbstractOperator operator = new ReconcileAllMockOperator(vertx, "TestResource", resourceOperator, metrics, null);

        Promise<Void> reconcileAllPromise = Promise.promise();
        ((ReconcileAllMockOperator) operator).setResources(resources);
        operator.reconcileAll("test", "my-namespace", reconcileAllPromise);

        Checkpoint async = context.checkpoint();
        reconcileAllPromise.future().onComplete(context.succeeding(v -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();
            Tag selectorTag = Tag.of("selector", "");

            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_PERIODICAL).meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_PERIODICAL).tag("kind", "TestResource").counter().count(), is(1.0));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_PERIODICAL).meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(3.0));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").counter().count(), is(3.0));

            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().count(), is(3L));
            assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            assertThat(registry.get(MetricsHolder.METRICS_RESOURCES).meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(MetricsHolder.METRICS_RESOURCES).tag("kind", "TestResource").tag("namespace", "my-namespace").gauge().value(), is(3.0));

            for (NamespaceAndName resource : resources) {
                assertThat(registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                        .tag("kind", "TestResource")
                        .tag("name", resource.getName())
                        .tag("resource-namespace", resource.getNamespace())
                        .gauge().value(), is(1.0));
            }

            async.flag();
        })));
    }

    @Test
    public void testReconcileAllOverMultipleNamespaces(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        Set<NamespaceAndName> resources = new HashSet<>(3);
        resources.add(new NamespaceAndName("my-namespace", "avfc"));
        resources.add(new NamespaceAndName("my-namespace", "vtid"));
        resources.add(new NamespaceAndName("my-namespace2", "utv"));

        Set<NamespaceAndName> updatedResources = new HashSet<>(2);
        updatedResources.add(new NamespaceAndName("my-namespace", "avfc"));
        updatedResources.add(new NamespaceAndName("my-namespace", "vtid"));

        AbstractWatchableStatusedNamespacedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithoutSelectorLabel();

        AbstractOperator operator = new ReconcileAllMockOperator(vertx, "TestResource", resourceOperator, metrics, null);

        Promise<Void> reconcileAllPromise = Promise.promise();
        ((ReconcileAllMockOperator) operator).setResources(resources);
        operator.reconcileAll("test", "*", reconcileAllPromise);

        Checkpoint async = context.checkpoint();
        reconcileAllPromise.future()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").tag("namespace", "my-namespace").counter().count(), is(2.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").tag("namespace", "my-namespace2").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").tag("namespace", "my-namespace").counter().count(), is(2.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").tag("namespace", "my-namespace2").counter().count(), is(1.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace").timer().count(), is(2L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace2").timer().count(), is(1L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace2").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCES).tag("kind", "TestResource").tag("namespace", "my-namespace").gauge().value(), is(2.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCES).tag("kind", "TestResource").tag("namespace", "my-namespace2").gauge().value(), is(1.0));

                    for (NamespaceAndName resource : resources) {
                        assertThat(registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                                .tag("kind", "TestResource")
                                .tag("name", resource.getName())
                                .tag("resource-namespace", resource.getNamespace())
                                .gauge().value(), is(1.0));
                    }
                })))
                .compose(ignore -> {
                    // Reconcile again with resource in my-namespace2 deleted
                    Promise<Void> secondReconcileAllPromise = Promise.promise();
                    ((ReconcileAllMockOperator) operator).setResources(updatedResources);
                    operator.reconcileAll("test", "*", secondReconcileAllPromise);

                    return secondReconcileAllPromise.future();
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_PERIODICAL).tag("kind", "TestResource").counter().count(), is(2.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").tag("namespace", "my-namespace").counter().count(), is(4.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS).tag("kind", "TestResource").tag("namespace", "my-namespace2").counter().count(), is(1.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").tag("namespace", "my-namespace").counter().count(), is(4.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL).tag("kind", "TestResource").tag("namespace", "my-namespace2").counter().count(), is(1.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace").timer().count(), is(4L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace2").timer().count(), is(1L));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RECONCILIATIONS_DURATION).tag("kind", "TestResource").tag("namespace", "my-namespace2").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCES).tag("kind", "TestResource").tag("namespace", "my-namespace").gauge().value(), is(2.0));
                    assertThat(registry.get(MetricsHolder.METRICS_RESOURCES).tag("kind", "TestResource").tag("namespace", "my-namespace2").gauge().value(), is(0.0));

                    for (NamespaceAndName resource : updatedResources) {
                        assertThat(registry.get(MetricsHolder.METRICS_RESOURCE_STATE)
                                .tag("kind", "TestResource")
                                .tag("name", resource.getName())
                                .tag("resource-namespace", resource.getNamespace())
                                .gauge().value(), is(1.0));
                    }

                    async.flag();
                })));
    }

    /**
     * Created new MetricsProvider and makes sure it doesn't contain any metrics from previous tests.
     *
     * @return  Clean MetricsProvider
     */
    public MetricsProvider createCleanMetricsProvider() {
        MetricsProvider metrics = new MicrometerMetricsProvider(BackendRegistries.getDefaultNow());
        MeterRegistry registry = metrics.meterRegistry();

        registry.forEachMeter(registry::remove);

        return metrics;
    }

    protected abstract static class MyResource extends CustomResource {
    }

    protected AbstractWatchableStatusedNamespacedResourceOperator resourceOperatorWithExistingResource(Labels selectorLabels)    {
        return new AbstractWatchableStatusedNamespacedResourceOperator(vertx, null, "TestResource") {
            @Override
            public Future updateStatusAsync(Reconciliation reconciliation, HasMetadata resource) {
                return null;
            }

            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public CustomResource get(String namespace, String name) {
                Foo foo = new Foo();
                ObjectMeta md = new ObjectMeta();
                if (selectorLabels != null) {
                    md.setLabels(selectorLabels.toMap());
                }
                foo.setMetadata(md);

                return foo;
            }

            @Override
            public Future getAsync(String namespace, String name) {
                return Future.succeededFuture(get(namespace, name));
            }
        };
    }

    private AbstractWatchableStatusedNamespacedResourceOperator resourceOperatorWithExistingResourceWithoutSelectorLabel() {
        return resourceOperatorWithExistingResource(null);
    }

    private AbstractWatchableStatusedNamespacedResourceOperator resourceOperatorWithExistingResourceWithSelectorLabel(Labels selectorLabel) {
        return resourceOperatorWithExistingResource(selectorLabel);
    }

    private AbstractWatchableStatusedNamespacedResourceOperator resourceOperatorWithExistingPausedResource() {
        return new AbstractWatchableStatusedNamespacedResourceOperator(vertx, null, "TestResource") {
            @Override
            public Future updateStatusAsync(Reconciliation reconciliation, HasMetadata resource) {
                return Future.succeededFuture();
            }

            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public CustomResource get(String namespace, String name) {
                Foo foo = new Foo();
                ObjectMeta md = new ObjectMeta();
                md.setAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"));
                foo.setMetadata(md);

                return foo;
            }

            @Override
            public Future getAsync(String namespace, String name) {
                Foo foo = new Foo();
                ObjectMeta md = new ObjectMeta();
                md.setAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"));
                foo.setMetadata(md);

                return Future.succeededFuture(foo);
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static class ReconcileAllMockOperator extends  AbstractOperator  {
        private Set<NamespaceAndName> resources;

        public ReconcileAllMockOperator(Vertx vertx, String kind, AbstractWatchableStatusedNamespacedResourceOperator resourceOperator, MetricsProvider metrics, Labels selectorLabels) {
            super(vertx, kind, resourceOperator, metrics, selectorLabels);
        }

        @Override
        protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
            return Future.succeededFuture();
        }

        public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
            return Future.succeededFuture(resources);
        }

        @Override
        Future<Void> updateStatus(Reconciliation reconciliation, Status desiredStatus) {
            return Future.succeededFuture();
        }

        @Override
        protected Future<Boolean> delete(Reconciliation reconciliation) {
            return null;
        }

        @Override
        protected Status createStatus(CustomResource ignored) {
            return new Status() { };
        }

        // Helper method used to set resources for each reconciliation
        public void setResources(Set<NamespaceAndName> resources) {
            this.resources = resources;
        }
    }

    @Group("strimzi")
    @Version("v1")
    public static class Foo extends MyResource {
        private ObjectMeta md;

        @Override
        public ObjectMeta getMetadata() {
            return md;
        }

        @Override
        public void setMetadata(ObjectMeta objectMeta) {
            md = objectMeta;
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

        @Override
        public Spec getSpec() {
            return new Spec() { };
        }

        @Override
        public void setSpec(Object spec) {
        }

        @Override
        public Status getStatus() {
            return null;
        }

        @Override
        public void setStatus(Object status) {

        }
    }
}