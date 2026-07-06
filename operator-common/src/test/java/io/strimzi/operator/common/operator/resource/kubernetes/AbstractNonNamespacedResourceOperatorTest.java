/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractNonNamespacedResourceOperatorTest<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {
    public static final String RESOURCE_NAME = "my-resource";
    protected static Executor asyncExecutor;

    @BeforeAll
    public static void before() {
        asyncExecutor = ForkJoinPool.commonPool();
    }

    /**
     * The type of kubernetes client to be mocked
     */
    protected abstract Class<C> clientType();

    /**
     * The type of the resource being tested
     */
    protected abstract Class<? extends Resource> resourceType();

    /**
     * Get a (new) test resource
     */
    protected abstract T resource();

    /**
     * Get a modified test resource to test how are changes handled
     */
    protected abstract T modifiedResource();

    /**
     * Configure the given {@code mockClient} to return the given {@code op}
     * that's appropriate for the kind of resource being tests.
     */
    protected abstract void mocker(C mockClient, MixedOperation op);

    /** Create the subclass of ResourceOperation to be tested */
    protected abstract AbstractNonNamespacedResourceOperator<C, T, L, R> createResourceOperations(
            C mockClient);

    /** Create the subclass of ResourceOperation to be tested with mocked readiness checks*/
    protected AbstractNonNamespacedResourceOperator<C, T, L, R> createResourceOperationsWithMockedReadiness(
            C mockClient)    {
        return createResourceOperations(mockClient);
    }

    @Test
    public void testCreateWhenExistsWithChangeIsAPatch() {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        HasMetadata hasMetadata = mock(HasMetadata.class);
        when(mockResource.get()).thenReturn(resource);

        when(mockResource.patch(any(), (T) any())).thenReturn(hasMetadata);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, modifiedResource())
            .<Void>handle((ar, error) -> {
                assertNull(error);
                verify(mockResource).get();
                verify(mockResource).patch(any(), (T) any());
                verify(mockResource, never()).create();
                verify(mockResource, never()).create();
                return null;
            }));
    }

    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch() {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);
        when(mockResource.patch(any(), any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource())
            .<Void>handle((ar, error) -> {
                assertNull(error);
                verify(mockResource).get();
                verify(mockResource, never()).patch(any(), any());
                verify(mockResource, never()).create();
                verify(mockResource, never()).create();
                return null;
            }));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenExistenceCheckThrows() {
        T resource = resource();
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource)
            .<Void>handle((ar, error) -> {
                assertThat(error, is(notNullValue()));
                assertThat(error, is(instanceOf(CompletionException.class)));
                assertThat(error.getCause(), is(ex));
                return null;
            }));
    }

    @Test
    public void testSuccessfulCreation() {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);
        when(mockResource.create()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);
        when(mockCms.resource(eq(resource))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperationsWithMockedReadiness(
                mockClient);

        TestUtils.await(op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource)
            .<Void>handle((rr, error) -> {
                assertNull(error);
                verify(mockResource).get();
                verify(mockResource).create();
                return null;
            }));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenCreateThrows() {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);
        when(mockResource.create()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);
        when(mockCms.resource(eq(resource))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource)
            .<Void>handle((rr, error) -> {
                assertThat(error, is(notNullValue()));
                assertThat(error, is(instanceOf(CompletionException.class)));
                assertThat(error.getCause(), is(ex));
                return null;
            }));
    }

    @Test
    public void testDeletionWhenResourceDoesNotExistIsANop() {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertNull(error);
                verify(mockResource).get();
                verify(mockResource, never()).delete();
                return null;
            }));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeletionWhenResourceExistsStillDeletes() {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(List.of());
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> {
                watchWasClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertNull(error);
                verify(mockResource).delete();
                assertThat("Watch was not closed", watchWasClosed.get(), is(true));
                return null;
            }));
    }

    @Test
    public void testReconcileThrowsWhenDeletionTimesOut() {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(List.of());
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            return (Watch) () -> {
                watchWasClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertThat(error, is(notNullValue()));
                assertThat(error, is(instanceOf(CompletionException.class)));
                assertThat(error.getCause(), is(instanceOf(TimeoutException.class)));

                verify(mockResource).delete();
                assertThat("Watch was not closed", watchWasClosed.get(), is(true));
                return null;
            }));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeletionSuccessful() {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(List.of());
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> {
                watchWasClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertNull(error);
                verify(mockResource).delete();
                assertThat("Watch was not closed", watchWasClosed.get(), is(true));
                return null;
            }));
    }

    @Test
    public void testReconcileDeletionThrowsWhenDeleteMethodThrows() {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenThrow(ex);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> {
                watchWasClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertThat(error, is(notNullValue()));
                assertThat(error, is(instanceOf(CompletionException.class)));
                assertThat(error.getCause(), is(ex));
                assertThat("Watch was not closed", watchWasClosed.get(), is(true));
                return null;
            }));
    }

    @Test
    public void testReconcileDeletionThrowsWhenWatchMethodThrows() {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenThrow(ex);
        when(mockResource.watch(any())).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertThat(error, is(notNullValue()));
                assertThat(error, is(instanceOf(CompletionException.class)));
                assertThat(error.getCause(), is(ex));
                return null;
            }));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeletionThrowsWhenDeleteMethodReturnsFalse() {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(List.of());
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher<T> watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, null);
            return (Watch) () -> {
                watchWasClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertNull(error);
                verify(mockResource).delete();
                assertThat("Watch was not closed", watchWasClosed.get(), is(true));
                return null;
            }));
    }

    // This tests the pre-check which should stop the self-closing-watch in case the resource is deleted before the
    // watch is opened.
    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeleteDoesNotTimeoutWhenResourceIsAlreadyDeleted() {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        AtomicBoolean watchCreated = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());

        when(mockResource.get()).thenAnswer(invocation -> {
            // First get needs to return the resource to trigger deletion
            // Next gets return null since the resource was already deleted
            if (watchCreated.get()) {
                return null;
            } else {
                return resource;
            }
        });
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
        when(mockResource.delete()).thenReturn(List.of());
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            watchCreated.set(true);
            return (Watch) () -> {
                watchWasClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(mockClient);

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null)
            .<Void>handle((rrDeleted, error) -> {
                assertNull(error);
                verify(mockResource).delete();
                assertThat("Watch was not closed", watchWasClosed.get(), is(true));
                return null;
            }));
    }
}
