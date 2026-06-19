/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.Util;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractReadyResourceOperatorTest<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList<T>, R extends Resource<T>> extends AbstractNamespacedResourceOperatorTest<C, T, L, R> {

    @Override
    protected abstract AbstractReadyNamespacedResourceOperator<C, T, L, R> createResourceOperations(C mockClient);

    @Test
    public void testReadinessThrowsWhenResourceDoesNotExist() {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.readiness(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, 20, 100)
            .<Void>handle((v, error) -> {
                assertThat(Util.maybeUnwrapCompletionException(error), instanceOf(TimeoutException.class));
                verify(mockResource, atLeastOnce()).get();
                verify(mockResource, never()).isReady();
                return null;
            }));
    }

    @Test
    public void testReadinessThrowsWhenExistenceCheckThrows() {
        T resource = resource();
        RuntimeException ex = new RuntimeException("This is a test exception");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.readiness(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, 20, 100)
            .<Void>handle((v, error) -> {
                assertThat(Util.maybeUnwrapCompletionException(error), instanceOf(TimeoutException.class));
                verify(mockResource, never()).isReady();
                return null;
            }));
    }

    @Test
    public void testWaitUntilReadySuccessfulImmediately() {
        waitUntilReadySuccessful(0);
    }

    @Test
    public void testWaitUntilReadySuccessfulAfterOneCall() {
        waitUntilReadySuccessful(1);
    }

    @Test
    public void testWaitUntilReadySuccessfulAfterTwoCalls() {
        waitUntilReadySuccessful(2);
    }

    public void waitUntilReadySuccessful(int unreadyCount) {
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
                throw new RuntimeException("The resource has already been ready once!");
            }
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.readiness(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, 20, 5_000)
            .<Void>handle((v, error) -> {
                assertNull(error);
                verify(mockResource, times(unreadyCount + 1)).isReady();
                return null;
            }));
    }

    @Test
    public void testWaitUntilReadyUnsuccessful() {
        T resource = resource();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.isReady()).thenReturn(Boolean.FALSE);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.readiness(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, 20, 100)
            .<Void>handle((v, error) -> {
                assertThat(Util.maybeUnwrapCompletionException(error), instanceOf(TimeoutException.class));
                verify(mockResource, atLeastOnce()).get();
                verify(mockResource, atLeastOnce()).isReady();
                return null;
            }));
    }

    @Test
    public void testWaitUntilReadyThrows() {
        T resource = resource();

        RuntimeException ex = new RuntimeException("This is a test exception");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource());
        when(mockResource.isReady()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractReadyNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.readiness(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, 20, 100)
            .<Void>handle((v, error) -> {
                assertThat(Util.maybeUnwrapCompletionException(error), instanceOf(TimeoutException.class));
                return null;
            }));
    }
}
