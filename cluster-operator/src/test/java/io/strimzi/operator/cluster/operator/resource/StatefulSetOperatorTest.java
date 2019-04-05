/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.apps.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
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
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static org.junit.Assert.assertTrue;
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
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };
    }

    @Override
    protected StatefulSetOperator createResourceOperationsWithMockedReadiness(Vertx vertx, KubernetesClient mockClient) {
        return new StatefulSetOperator(vertx, mockClient, 60_000L) {
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
    public void createWhenExistsIsAPatch(TestContext context) {
        createWhenExistsIsAPatch(context, false);
    }

    @Test
    public void rollingUpdateSuccess() {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));

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
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0", pod -> true);
        assertTrue(result.succeeded());
    }
    @Test
    public void rollingUpdateDeletionTimeout() {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.failedFuture(new TimeoutException()));
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
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0", pod -> true);
        assertTrue(result.failed());
        assertTrue(result.cause() instanceof TimeoutException);
    }

    @Test
    public void rollingUpdateReadinessTimeout() {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.failedFuture(new TimeoutException()));
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));

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
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0", pod -> true);
        assertTrue(result.failed());
        assertTrue(result.cause() instanceof TimeoutException);
    }

    @Test
    public void rollingUpdateReconcileFailed() {
        StatefulSet resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.waitFor(anyString(), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(podOperator.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.failedFuture("reconcile failed"));
        when(podOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(new PodBuilder().withNewMetadata().withName("my-pod-0").endMetadata().build()));

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
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0", pod -> true);
        assertTrue(result.failed());
        assertTrue(result.cause().getMessage().equals("reconcile failed"));
    }

    @Test
    public void testInternalReplace(TestContext context)   {
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

        EditReplacePatchDeletable mockERPD = mock(EditReplacePatchDeletable.class);

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(sts1);
        when(mockResource.cascading(eq(false))).thenReturn(mockERPD);

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
            protected boolean shouldIncrementGeneration(StatefulSetDiff diff) {
                return true;
            }

            @Override
            public Future<Void> waitFor(String namespace, String name, long pollIntervalMs, final long timeoutMs, BiPredicate<String, String> predicate) {
                return Future.succeededFuture();
            }
        };

        Async async = context.async();
        op.reconcile(sts1.getMetadata().getNamespace(), sts1.getMetadata().getName(), sts2).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            assertTrue(ar.succeeded());
            verify(mockERPD).delete();
            async.complete();
        });
    }
}
