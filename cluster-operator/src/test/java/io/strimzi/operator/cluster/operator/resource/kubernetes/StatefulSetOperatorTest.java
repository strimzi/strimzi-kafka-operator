/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.client.GracePeriodConfigurable;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.exceptions.base.MockitoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatefulSetOperatorTest extends ScalableResourceOperatorTest<KubernetesClient, StatefulSet, StatefulSetList,
                                RollableScalableResource<StatefulSet>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<RollableScalableResource> resourceType() {
        return RollableScalableResource.class;
    }

    @Override
    protected StatefulSet resource(String name) {
        return new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace(AbstractNamespacedResourceOperatorTest.NAMESPACE)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewTemplate()
                        .withNewMetadata()
                            .addToAnnotations(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_GENERATION, "1")
                        .endMetadata()
                    .endTemplate()
                .endSpec()
                .build();
    }

    @Override
    protected StatefulSet modifiedResource(String name) {
        return new StatefulSetBuilder(resource(name))
                .editSpec()
                    .withReplicas(5)
                    .withNewTemplate()
                        .withNewMetadata()
                            .addToAnnotations(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_GENERATION, "2")
                        .endMetadata()
                        .withNewSpec()
                            .withHostname("new-hostname")
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
        when(mockExt.statefulSets()).thenReturn(op);
        when(mockClient.apps()).thenReturn(mockExt);
    }

    @Override
    protected StatefulSetOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new StatefulSetOperator(vertx, mockClient, 60_000L) {
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return false;
            }
        };
    }

    @Override
    protected StatefulSetOperator createResourceOperationsWithMockedReadiness(Vertx vertx, KubernetesClient mockClient) {
        return new StatefulSetOperator(vertx, mockClient, 60_000L) {
            @Override
            public Future<Void> readiness(Reconciliation reconciliation, String namespace, String name, long pollIntervalMs, long timeoutMs) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }

            @Override
            protected Future<?> podReadiness(Reconciliation reconciliation, String namespace, StatefulSet desired, long pollInterval, long operationTimeoutMs) {
                return Future.succeededFuture();
            }
        };
    }

    @Override
    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        testCreateWhenExistsWithChangeIsAPatch(context, false);
    }

    @Override
    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context) {
        testCreateWhenExistsWithoutChangeIsNotAPatch(context, false);
    }

    @Test
    public void testInternalReplace(VertxTestContext context)   {
        StatefulSet sts1 = new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace(AbstractNamespacedResourceOperatorTest.NAMESPACE)
                    .withName(AbstractNamespacedResourceOperatorTest.RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewTemplate()
                        .withNewMetadata()
                        .endMetadata()
                    .endTemplate()
                .endSpec()
                .build();

        Map<String, Quantity> requests = new HashMap<>();
        requests.put("storage", new Quantity("100Gi"));

        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName("data")
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(requests)
                    .endResources()
                    .withStorageClassName("gp2")
                .endSpec()
                .build();

        StatefulSet sts2 = new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace(AbstractNamespacedResourceOperatorTest.NAMESPACE)
                    .withName(AbstractNamespacedResourceOperatorTest.RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewTemplate()
                        .withNewMetadata()
                        .endMetadata()
                    .endTemplate()
                    .withVolumeClaimTemplates(pvc)
                .endSpec()
                .build();

        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());

        GracePeriodConfigurable gpc = mock(GracePeriodConfigurable.class);
        when(gpc.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(sts1);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.ORPHAN))).thenReturn(gpc);
        when(mockResource.create()).thenReturn(sts1);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(any(), anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);
        when(mockNameable.resource(eq(sts2))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractNamespacedResourceOperatorTest.vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }

            @Override
            public Future<Void> waitFor(Reconciliation reconciliation, String namespace, String name, String logState, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test", "kind", "namespace", "name"), sts1.getMetadata().getNamespace(), sts1.getMetadata().getName(), sts2)
            .onComplete(context.succeeding(rrState -> {
                verify(mockDeletable).delete();
                async.flag();
            }));
    }

    @Test
    public void testCascadingDeleteAsync(VertxTestContext context)   {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());

        GracePeriodConfigurable gpc = mock(GracePeriodConfigurable.class);
        when(gpc.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        ArgumentCaptor<DeletionPropagation> cascadingCaptor = ArgumentCaptor.forClass(DeletionPropagation.class);
        when(mockRSR.withPropagationPolicy(cascadingCaptor.capture())).thenReturn(gpc);

        ArgumentCaptor<Watcher> watcherCaptor = ArgumentCaptor.forClass(Watcher.class);
        when(mockRSR.watch(watcherCaptor.capture())).thenReturn(mock(Watch.class));

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractNamespacedResourceOperatorTest.vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint async = context.checkpoint();
        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, true)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cascadingCaptor.getValue(), is(DeletionPropagation.FOREGROUND));
                async.flag();
            })));
    }

    @Test
    public void testNonCascadingDeleteAsync(VertxTestContext context)   {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());

        GracePeriodConfigurable gpc = mock(GracePeriodConfigurable.class);
        when(gpc.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        ArgumentCaptor<DeletionPropagation> cascadingCaptor = ArgumentCaptor.forClass(DeletionPropagation.class);
        when(mockRSR.withPropagationPolicy(cascadingCaptor.capture())).thenReturn(gpc);
        ArgumentCaptor<Watcher> watcherCaptor = ArgumentCaptor.forClass(Watcher.class);
        when(mockRSR.watch(watcherCaptor.capture())).thenReturn(mock(Watch.class));

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractNamespacedResourceOperatorTest.vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cascadingCaptor.getValue(), is(DeletionPropagation.ORPHAN));
                a.flag();
            })));
    }

    @Test
    public void testDeleteAsyncNotDeleted(VertxTestContext context)   {
        GracePeriodConfigurable mockERPD = mock(GracePeriodConfigurable.class);
        when(mockERPD.delete()).thenReturn(List.of());

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        when(mockRSR.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockERPD);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractNamespacedResourceOperatorTest.vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.failing(e -> a.flag()));
    }

    @Test
    public void testDeleteAsyncFailing(VertxTestContext context)   {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenThrow(new MockitoException("Something failed"));

        GracePeriodConfigurable gpc = mock(GracePeriodConfigurable.class);
        when(gpc.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        when(mockRSR.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(gpc);
        when(mockRSR.watch(any())).thenReturn(mock(Watch.class));

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractNamespacedResourceOperatorTest.vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint async = context.checkpoint();
        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(MockitoException.class));
                assertThat(e.getMessage(), is("Something failed"));
                async.flag();
            })));
    }

    @Override
    @Test
    public void testBatchReconciliation(VertxTestContext context) {
        assumeTrue(false, "StatefulSetOperator reconciliation uses custom code. This test should be skipped.");
    }
}
