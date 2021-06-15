/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.*;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.*;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.exceptions.base.MockitoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("rawtypes")
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
                            .addToAnnotations(AbstractScalableResourceOperator.ANNO_STRIMZI_IO_GENERATION, "1")
                        .endMetadata()
                    .endTemplate()
                .endSpec()
                .build();
    }

    @Override
    protected StatefulSet modifiedResource() {
        return new StatefulSetBuilder(resource())
                .editSpec()
                    .withReplicas(5)
                    .withNewTemplate()
                        .withNewMetadata()
                            .addToAnnotations(AbstractScalableResourceOperator.ANNO_STRIMZI_IO_GENERATION, "2")
                        .endMetadata()
                        .withNewSpec()
                            .withHostname("new-hostname")
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    @SuppressWarnings("unchecked")
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
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

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
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

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

    @SuppressWarnings("unchecked")
    @Test
    public void testRollingUpdateSuccess(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(any(), anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));
        when(podOperator.restart(any(), any(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.maybeRestartPod(new Reconciliation("test", "kind", "namespace", "name"), resource, "my-pod-0", pod -> singletonList("roll"))
            .onComplete(context.succeeding(v -> context.completeNow()));
    }

    @Test
    public void testRollingUpdateDeletionTimeout(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        AtomicInteger call = new AtomicInteger();
        when(podOperator.getAsync(anyString(), anyString())).thenAnswer(invocation -> {
            if (call.getAndIncrement() == 0) {
                return Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build());
            } else {
                return null;
            }
        });
        when(podOperator.restart(any(), any(), anyLong())).thenReturn(Future.failedFuture(new TimeoutException()));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.maybeRestartPod(new Reconciliation("test", "kind", "namespace", "name"), resource, "my-pod-0", pod -> singletonList("roll"))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(TimeoutException.class));
                context.completeNow();
            })));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRollingUpdateReadinessTimeout(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(any(), anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.failedFuture(new TimeoutException()));
        when(podOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));
        when(podOperator.restart(any(), any(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.maybeRestartPod(new Reconciliation("test", "kind", "namespace", "name"), resource, "my-pod-0", pod -> singletonList("roll")).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, instanceOf(TimeoutException.class));
            context.completeNow();
        })));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRollingUpdateReconcileFailed(VertxTestContext context) {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(any(), anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));
        when(podOperator.restart(any(), any(), anyLong())).thenReturn(Future.failedFuture("reconcile failed"));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }
            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.maybeRestartPod(new Reconciliation("test", "kind", "namespace", "name"), resource, "my-pod-0", pod -> singletonList("roll"))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e.getMessage(), is("reconcile failed"));
                context.completeNow();
            })));
    }

    @SuppressWarnings("unchecked")
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
        when(podOperator.waitFor(any(), anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));

        PvcOperator pvcOperator = mock(PvcOperator.class);
        when(pvcOperator.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(AbstractResourceOperatorTest.vertx, mockClient, 5_000L, podOperator, pvcOperator) {
            @Override
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }

            @Override
            public Future<Void> waitFor(Reconciliation reconciliation, String namespace, String name, String logState, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
                return Future.succeededFuture();
            }
        };

        op.reconcile(new Reconciliation("test", "kind", "namespace", "name"), sts1.getMetadata().getNamespace(), sts1.getMetadata().getName(), sts2)
            .onComplete(context.succeeding(rrState -> {
                verify(mockDeletable).delete();
                context.completeNow();
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
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, true)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cascadingCaptor.getValue(), is(DeletionPropagation.FOREGROUND));
                context.completeNow();
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
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cascadingCaptor.getValue(), is(DeletionPropagation.ORPHAN));
                context.completeNow();
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
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.failing(e -> context.completeNow()));
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
            public Future<Void> maybeRollingUpdate(Reconciliation reconciliation, StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, Secret clusterCaSecret, Secret coKeySecret) {
                return Future.succeededFuture();
            }

            @Override
            protected boolean shouldIncrementGeneration(Reconciliation reconciliation, StatefulSetDiff diff) {
                return true;
            }
        };

        op.deleteAsync(new Reconciliation("test", "kind", "namespace", "name"), NAMESPACE, RESOURCE_NAME, false)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(MockitoException.class));
                assertThat(e.getMessage(), is("Something failed"));
                context.completeNow();
            })));
    }
}
