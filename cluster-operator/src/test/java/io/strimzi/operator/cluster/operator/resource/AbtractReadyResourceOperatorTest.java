/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbtractReadyResourceOperatorTest<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList, D extends Doneable<T>, R extends Resource<T, D>> extends AbstractResourceOperatorTest<C, T, L, D, R> {

    @Override
    protected abstract AbstractReadyResourceOperator<C, T, L, D, R> createResourceOperations(Vertx vertx, C mockClient);

    @Test
    public void waitUntilReadyWhenDoesNotExist(TestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);
        Async async = context.async();
        Future<Void> fut = op.readiness(NAMESPACE, RESOURCE_NAME, 20, 100);
        fut.setHandler(ar -> {
            assertTrue(ar.failed());
            assertThat(ar.cause(), instanceOf(TimeoutException.class));
            verify(mockResource, atLeastOnce()).get();
            verify(mockResource, never()).isReady();
            async.complete();
        });

    }

    @Test
    public void waitUntilReadyExistenceCheckThrows(TestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.readiness(NAMESPACE, RESOURCE_NAME, 20, 100).setHandler(ar -> {
            assertTrue(ar.failed());
            assertThat(ar.cause(), instanceOf(TimeoutException.class));
            verify(mockResource, never()).isReady();
            async.complete();
        });
    }

    @Test
    public void waitUntilReadySuccessfulImmediately(TestContext context) {
        waitUntilReadySuccessful(context, 0);
    }

    @Test
    public void waitUntilReadySuccessfulAfterOneCall(TestContext context) {
        waitUntilReadySuccessful(context, 1);
    }

    @Test
    public void waitUntilReadySuccessfulAfterTwoCalls(TestContext context) {
        waitUntilReadySuccessful(context, 2);
    }

    public void waitUntilReadySuccessful(TestContext context, int unreadyCount) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        AtomicInteger count = new AtomicInteger();
        when(mockResource.isReady()).then(invocation -> {
            int cnt = count.getAndIncrement();
            if (cnt < unreadyCount) {
                return Boolean.FALSE;
            } else if (cnt == unreadyCount) {
                return Boolean.TRUE;
            } else {
                context.fail("The resource has already been ready once!");
            }
            throw new RuntimeException();
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.readiness(NAMESPACE, RESOURCE_NAME, 20, 5_000).setHandler(ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource, times(Readiness.isReadinessApplicable(resource.getClass()) ? unreadyCount + 1 : 1)).get();

            if (Readiness.isReadinessApplicable(resource.getClass())) {
                verify(mockResource, times(unreadyCount + 1)).isReady();
            }
            async.complete();
        });
    }

    @Test
    public void waitUntilReadyUnsuccessful(TestContext context) {
        T resource = resource();

        if (!Readiness.isReadinessApplicable(resource.getClass()))  {
            return;
        }

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.isReady()).thenReturn(Boolean.FALSE);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.readiness(NAMESPACE, RESOURCE_NAME, 20, 100).setHandler(ar -> {
            assertTrue(ar.failed());
            assertThat(ar.cause(), instanceOf(TimeoutException.class));
            verify(mockResource, atLeastOnce()).get();
            verify(mockResource, atLeastOnce()).isReady();
            async.complete();
        });
    }

    @Test
    public void waitUntilReadyThrows(TestContext context) {
        T resource = resource();

        if (!Readiness.isReadinessApplicable(resource.getClass()))  {
            return;
        }

        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource());
        when(mockResource.isReady()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.readiness(NAMESPACE, RESOURCE_NAME, 20, 100).setHandler(ar -> {
            assertTrue(ar.failed());
            assertThat(ar.cause(), instanceOf(TimeoutException.class));
            async.complete();
        });
    }
}
