/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.function.BiPredicate;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
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
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
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
            protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
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
            protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
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

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
                return true;
            }

            @Override
            protected boolean isPodUpToDate(StatefulSet ss, String podName) {
                return false;
            }

            @Override
            protected Future<Integer> getGeneration(String namespace, String podName) {
                return Future.succeededFuture(1);
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0");
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

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
                return true;
            }

            @Override
            protected boolean isPodUpToDate(StatefulSet ss, String podName) {
                return false;
            }

            @Override
            protected Future<Integer> getGeneration(String namespace, String podName) {
                return Future.succeededFuture(1);
            }

        };

        Future result = op.maybeRestartPod(resource, "my-pod-0");
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

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
                return true;
            }

            @Override
            protected boolean isPodUpToDate(StatefulSet ss, String podName) {
                return false;
            }

            @Override
            protected Future<Integer> getGeneration(String namespace, String podName) {
                return Future.succeededFuture(1);
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0");
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

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        StatefulSetOperator op = new StatefulSetOperator(vertx, mockClient, 5_000L, podOperator) {
            @Override
            protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
                return true;
            }

            @Override
            protected boolean isPodUpToDate(StatefulSet ss, String podName) {
                return false;
            }

            @Override
            protected Future<Integer> getGeneration(String namespace, String podName) {
                return Future.succeededFuture(1);
            }
        };

        Future result = op.maybeRestartPod(resource, "my-pod-0");
        assertTrue(result.failed());
        assertTrue(result.cause().getMessage().equals("reconcile failed"));
    }
}
