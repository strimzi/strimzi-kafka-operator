/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

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
import io.strimzi.operator.common.TimeoutException;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public abstract class AbstractNonNamespacedResourceOperatorTest<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {
    public static final String RESOURCE_NAME = "my-resource";
    protected static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
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
            Vertx vertx, C mockClient);

    /** Create the subclass of ResourceOperation to be tested with mocked readiness checks*/
    protected AbstractNonNamespacedResourceOperator<C, T, L, R> createResourceOperationsWithMockedReadiness(
            Vertx vertx, C mockClient)    {
        return createResourceOperations(vertx, mockClient);
    }

    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, modifiedResource())
                .onComplete(context.succeeding(ar -> {
                    verify(mockResource).get();
                    verify(mockResource).patch(any(), (T) any());
                    verify(mockResource, never()).create();
                    verify(mockResource, never()).create();
                    async.flag();
                }));
    }

    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource())
                .onComplete(context.succeeding(ar -> {
                    verify(mockResource).get();
                    verify(mockResource, never()).patch(any(), any());
                    verify(mockResource, never()).create();
                    verify(mockResource, never()).create();
                    async.flag();
                }));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenExistenceCheckThrows(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            async.flag();
        }));
    }

    @Test
    public void testSuccessfulCreation(VertxTestContext context) {
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
                vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.succeeding(rr -> {
            verify(mockResource).get();
            verify(mockResource).create();
            async.flag();
        }));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenCreateThrows(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            async.flag();
        }));
    }

    @Test
    public void testDeletionWhenResourceDoesNotExistIsANop(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            async.flag();
        }));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeletionWhenResourceExistsStillDeletes(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        }));
    }

    @Test
    public void testReconcileThrowsWhenDeletionTimesOut(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, instanceOf(TimeoutException.class));
            verify(mockResource).delete();
            assertThat("Watch was not closed", watchWasClosed.get(), is(true));
            async.flag();
        })));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeletionSuccessful(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        }));
    }

    @Test
    public void testReconcileDeletionThrowsWhenDeleteMethodThrows(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, is(ex));
            assertThat("Watch was not closed", watchWasClosed.get(), is(true));
            async.flag();
        })));
    }

    @Test
    public void testReconcileDeletionThrowsWhenWatchMethodThrows(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            async.flag();
        }));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeletionThrowsWhenDeleteMethodReturnsFalse(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        }));
    }

    // This tests the pre-check which should stop the self-closing-watch in case the resource is deleted before the
    // watch is opened.
    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeleteDoesNotTimeoutWhenResourceIsAlreadyDeleted(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getName(), null).onComplete(context.succeeding(rrDeleted -> {
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        }));
    }
}

