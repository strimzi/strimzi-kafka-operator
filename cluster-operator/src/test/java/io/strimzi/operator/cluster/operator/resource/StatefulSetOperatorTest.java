/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.common.operator.resource.AbstractResourceOperatorTest;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ScalableResourceOperatorTest;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.exceptions.base.MockitoException;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatefulSetOperatorTest
        extends ScalableResourceOperatorTest<KubernetesClient, StatefulSet, StatefulSetList,
                                DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<RollableScalableResource> resourceType() {
        return RollableScalableResource.class;
    }

    @Override
    protected StatefulSet resource() {
        return new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace(AbstractResourceOperatorTest.NAMESPACE)
                    .withName(AbstractResourceOperatorTest.RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewTemplate()
                        .withNewMetadata()
                        .endMetadata()
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
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };
    }

    @Override
    protected StatefulSetOperator createResourceOperationsWithMockedReadiness(Vertx vertx, KubernetesClient mockClient) {
        return new StatefulSetOperator(vertx, mockClient, 60_000L) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            public Future<Void> readiness(String namespace, String name, long pollIntervalMs, long timeoutMs) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }

            @Override
            protected Future<?> podReadiness(String namespace, StatefulSet desired, long pollInterval, long operationTimeoutMs) {
                return Future.succeededFuture();
            }
        };
    }

    @Override
    @Test
    public void testCreateWhenExistsIsAPatch(VertxTestContext context) {
        createWhenExistsIsAPatch(context, false);
    }

    @Test
    public void testRollingUpdateSuccess(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));
        when(podOperator.restart(anyString(), any(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.maybeRestartPod(resource, "my-pod-0", pod -> singletonList("roll"))
            .onComplete(context.succeeding(v -> a.flag()));
    }

    @Test
    public void testRollingUpdateDeletionTimeout(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        AtomicInteger call = new AtomicInteger();
        when(podOperator.getAsync(anyString(), anyString())).thenAnswer(invocation -> {
            if (call.getAndIncrement() == 0) {
                return Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build());
            } else {
                return null;
            }
        });
        when(podOperator.restart(anyString(), any(), anyLong())).thenReturn(Future.failedFuture(new TimeoutException()));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.maybeRestartPod(resource, "my-pod-0", pod -> singletonList("roll"))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(TimeoutException.class));
                a.flag();
            })));
    }

    @Test
    public void testRollingUpdateReadinessTimeout(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.failedFuture(new TimeoutException()));
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));
        when(podOperator.restart(anyString(), any(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }
            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.maybeRestartPod(resource, "my-pod-0", pod -> singletonList("roll")).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, instanceOf(TimeoutException.class));
            a.flag();
        })));
    }

    @Test
    public void testRollingUpdateReconcileFailed(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));
        when(podOperator.restart(anyString(), any(), anyLong())).thenReturn(Future.failedFuture("reconcile failed"));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }
            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.maybeRestartPod(resource, "my-pod-0", pod -> singletonList("roll"))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e.getMessage(), is("reconcile failed"));
                a.flag();
            })));
    }

    @Test
    public void testInternalReplace(VertxTestContext context)   {
        StatefulSet sts1 = new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace(AbstractResourceOperatorTest.NAMESPACE)
                    .withName(AbstractResourceOperatorTest.RESOURCE_NAME)
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
                    .withNamespace(AbstractResourceOperatorTest.NAMESPACE)
                    .withName(AbstractResourceOperatorTest.RESOURCE_NAME)
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
        when(mockDeletable.delete()).thenReturn(Boolean.TRUE);

        Resource mockERPD = mock(resourceType());
        when(mockERPD.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockDeletable);
        when(mockERPD.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(sts1);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.ORPHAN))).thenReturn(mockERPD);
        when(mockResource.create(any(StatefulSet.class))).thenReturn(sts1);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }

            @Override
            public Future<Void> waitFor(String namespace, String name, String logState, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        op.reconcile(sts1.getMetadata().getNamespace(), sts1.getMetadata().getName(), sts2)
            .onComplete(context.succeeding(rrState -> {
                verify(mockDeletable).delete();
                async.flag();
            }));
    }

    @Test
    public void testCascadingDeleteAsync(VertxTestContext context)   {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(Boolean.TRUE);

        Resource mockERPD = mock(resourceType());
        when(mockERPD.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockDeletable);
        when(mockERPD.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        ArgumentCaptor<DeletionPropagation> cascadingCaptor = ArgumentCaptor.forClass(DeletionPropagation.class);
        when(mockRSR.withPropagationPolicy(cascadingCaptor.capture())).thenReturn(mockERPD);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);
        PvcOperator pvcOperator = mock(PvcOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint async = context.checkpoint();
        op.deleteAsync(NAMESPACE, RESOURCE_NAME, true)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cascadingCaptor.getValue(), is(DeletionPropagation.FOREGROUND));
                async.flag();
            })));
    }

    @Test
    public void testNonCascadingDeleteAsync(VertxTestContext context)   {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(Boolean.TRUE);

        Resource mockERPD = mock(resourceType());
        when(mockERPD.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockDeletable);
        when(mockERPD.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        ArgumentCaptor<DeletionPropagation> cascadingCaptor = ArgumentCaptor.forClass(DeletionPropagation.class);
        when(mockRSR.withPropagationPolicy(cascadingCaptor.capture())).thenReturn(mockERPD);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);
        PvcOperator pvcOperator = mock(PvcOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.deleteAsync(NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cascadingCaptor.getValue(), is(DeletionPropagation.ORPHAN));
                a.flag();
            })));
    }

    @Test
    public void testDeleteAsyncNotDeleted(VertxTestContext context)   {
        EditReplacePatchDeletable mockERPD = mock(EditReplacePatchDeletable.class);
        when(mockERPD.delete()).thenReturn(Boolean.FALSE);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        when(mockRSR.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockERPD);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);
        PvcOperator pvcOperator = mock(PvcOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint a = context.checkpoint();
        op.deleteAsync(NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.failing(e -> a.flag()));
    }

    @Test
    public void testDeleteAsyncFailing(VertxTestContext context)   {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenThrow(new MockitoException("Something failed"));

        Resource mockERPD = mock(resourceType());
        when(mockERPD.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockDeletable);
        when(mockERPD.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        RollableScalableResource mockRSR = mock(RollableScalableResource.class);
        when(mockRSR.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(mockERPD);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockRSR);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        PodOperator podOperator = mock(PodOperator.class);
        PvcOperator pvcOperator = mock(PvcOperator.class);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Checkpoint async = context.checkpoint();
        op.deleteAsync(NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(MockitoException.class));
                assertThat(e.getMessage(), is("Something failed"));
                async.flag();
            })));
    }
}
