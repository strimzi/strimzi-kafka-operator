/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.GracePeriodConfigurable;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public abstract class AbstractNamespacedResourceOperatorTest<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList<T>, R extends Resource<T>> {

    public static final String RESOURCE_NAME = "my-resource";
    public static final String NAMESPACE = "test";
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
     * @return  New resource with the default name
     */
    protected T resource()  {
        return resource(RESOURCE_NAME);
    }

    /**
     * Create a resource which will be used for the tests
     *
     * @param name  Name of the resource
     *
     * @return  New resource with the name
     */
    protected abstract T resource(String name);

    /**
     * @return  Modified resource with the default name
     */
    protected T modifiedResource()  {
        return modifiedResource(RESOURCE_NAME);
    }

    /**
     * Create a modified resource which will be used for the tests
     *
     * @param name Name of the resource
     * @return Modified resource with the name
     */
    protected abstract T modifiedResource(String name);

    /**
     * Configure the given {@code mockClient} to return the given {@code op}
     * that's appropriate for the kind of resource being tests.
     */
    protected abstract void mocker(C mockClient, MixedOperation op);

    /** Create the subclass of ResourceOperation to be tested */
    protected abstract AbstractNamespacedResourceOperator<C, T, L, R> createResourceOperations(Vertx vertx, C mockClient);

    /** Create the subclass of ResourceOperation to be tested with mocked readiness checks*/
    protected AbstractNamespacedResourceOperator<C, T, L, R> createResourceOperationsWithMockedReadiness(Vertx vertx, C mockClient)    {
        return createResourceOperations(vertx, mockClient);
    }

    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        testCreateWhenExistsWithChangeIsAPatch(context, true);
    }

    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context, boolean cascade) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        when(mockResource.patch(any(), (T) any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, modifiedResource()).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource).patch(any(), (T) any());
            verify(mockResource, never()).create();
            verify(mockResource, never()).createOrReplace();
            async.flag();
        })));
    }

    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context) {
        testCreateWhenExistsWithoutChangeIsNotAPatch(context, true);
    }

    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context, boolean cascade) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(cascade ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN)).thenReturn(mockResource);
        when(mockResource.patch(any(), any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource()).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource, never()).patch(any(), any());
            verify(mockResource, never()).create();
            verify(mockResource, never()).createOrReplace();
            async.flag();
        })));
    }

    @Test
    public void testExistenceCheckThrows(VertxTestContext context) {
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

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, is(ex));
            async.flag();
        })));
    }

    @Test
    public void testSuccessfulCreation(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        when(mockResource.get()).thenReturn(null);
        when(mockResource.create()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);
        when(mockNameable.resource(eq(resource))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperationsWithMockedReadiness(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource).create();
            async.flag();
        })));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenCreateThrows(VertxTestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);
        when(mockNameable.resource(eq(resource))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);
        when(mockResource.create()).thenThrow(ex);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            async.flag();
        }));
    }

    @Test
    public void testDeleteWhenResourceDoesNotExistIsANop(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.succeeding(rr -> context.verify(() -> {
                verify(mockResource).get();
                verify(mockResource, never()).delete();
                async.flag();
            })));
    }

    @Test
    public void testReconcileDeleteWhenResourceExistsStillDeletes(VertxTestContext context) {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());
        GracePeriodConfigurable mockDeletableGrace = mock(GracePeriodConfigurable.class);
        when(mockDeletableGrace.delete()).thenReturn(List.of());

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> {
                watchClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.succeeding(rr -> context.verify(() -> {
                verify(mockDeletable).delete();
                async.flag();
            })));
    }

    @Test
    public void testReconcileDeletionSuccessfullyDeletes(VertxTestContext context) {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());
        GracePeriodConfigurable mockDeletableGrace = mock(GracePeriodConfigurable.class);
        when(mockDeletableGrace.delete()).thenReturn(List.of());

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> {
                watchClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.succeeding(rr -> context.verify(() -> {
                verify(mockDeletable).delete();
                async.flag();
            })));
    }

    @Test
    public void testReconcileDeleteThrowsWhenDeletionThrows(VertxTestContext context) {
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");
        Deletable mockDeletable = mock(Deletable.class);
        GracePeriodConfigurable mockDeletableGrace = mock(GracePeriodConfigurable.class);
        when(mockDeletable.delete()).thenThrow(ex);

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> {
                watchClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, is(ex));
                async.flag();
            })));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeleteDoesNotThrowWhenDeletionReturnsFalse(VertxTestContext context) {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());
        GracePeriodConfigurable mockDeletableGrace = mock(GracePeriodConfigurable.class);
        when(mockDeletableGrace.delete()).thenReturn(List.of());

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> {
                watchClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
                .onComplete(context.succeeding(rr -> context.verify(() -> {
                    verify(mockDeletable).delete();
                    async.flag();
                })));
    }

    // This tests the pre-check which should stop the self-closing-watch in case the resource is deleted before the
    // watch is opened.
    @Test
    @SuppressWarnings("unchecked")
    public void testReconcileDeleteDoesNotTimeoutWhenResourceIsAlreadyDeleted(VertxTestContext context) {
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());
        GracePeriodConfigurable mockDeletableGrace = mock(GracePeriodConfigurable.class);
        when(mockDeletableGrace.delete()).thenReturn(List.of());

        T resource = resource();
        Resource mockResource = mock(resourceType());
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        AtomicBoolean watchCreated = new AtomicBoolean(false);

        when(mockResource.get()).thenAnswer(invocation -> {
            // First get needs to return the resource to trigger deletion
            // Next gets return null since the resource was already deleted
            if (watchCreated.get()) {
                return null;
            } else {
                return resource;
            }
        });
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        when(mockResource.watch(any())).thenAnswer(invocation -> {
            watchCreated.set(true);
            return (Watch) () -> {
                watchClosed.set(true);
            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
                .onComplete(context.succeeding(rr -> context.verify(() -> {
                    verify(mockDeletable).delete();
                    async.flag();
                })));
    }

    @Test
    public void testBatchReconciliation(VertxTestContext context) {
        Map<String, String> selector = Map.of("labelA", "a", "labelB", "b");

        T resource1 = resource("resource-1");
        T resource2 = resource("resource-2");
        T resource2Mod = modifiedResource("resource-2");
        T resource3 = resource("resource-3");

        // For resource1 we need to mock the async deletion process as well
        Deletable mockDeletable1 = mock(Deletable.class);
        when(mockDeletable1.delete()).thenReturn(List.of());
        GracePeriodConfigurable mockDeletableGrace1 = mock(GracePeriodConfigurable.class);
        when(mockDeletableGrace1.withGracePeriod(anyLong())).thenReturn(mockDeletable1);
        Resource mockResource1 = mock(resourceType());
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        AtomicBoolean watchCreated = new AtomicBoolean(false);
        when(mockResource1.get()).thenAnswer(invocation -> {
            // First get needs to return the resource to trigger deletion
            // Next gets return null since the resource was already deleted
            if (watchCreated.get()) {
                return null;
            } else {
                return resource1;
            }
        });
        when(mockResource1.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockDeletableGrace1);
        when(mockResource1.watch(any())).thenAnswer(invocation -> {
            watchCreated.set(true);
            return (Watch) () -> {
                watchClosed.set(true);
            };
        });

        Resource mockResource2 = mock(resourceType());
        when(mockResource2.get()).thenReturn(resource2);
        when(mockResource2.patch(any(), eq(resource2Mod))).thenReturn(resource2Mod);

        Resource mockResource3 = mock(resourceType());
        when(mockResource3.get()).thenReturn(null);
        when(mockResource3.create()).thenReturn(resource3);

        KubernetesResourceList mockResourceList = mock(KubernetesResourceList.class);
        when(mockResourceList.getItems()).thenReturn(List.of(resource1, resource2));

        FilterWatchListDeletable mockListable = mock(FilterWatchListDeletable.class);
        when(mockListable.list(any())).thenReturn((L) mockResourceList);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withLabels(eq(selector))).thenReturn(mockListable);
        when(mockNameable.withName(eq("resource-1"))).thenReturn(mockResource1);
        when(mockNameable.withName(eq("resource-2"))).thenReturn(mockResource2);
        when(mockNameable.withName(eq("resource-3"))).thenReturn(mockResource3);
        when(mockNameable.resource(eq(resource3))).thenReturn(mockResource3);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(anyString())).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.batchReconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, List.of(resource2Mod, resource3), Labels.fromMap(selector)).onComplete(context.succeeding(i -> context.verify(() -> {
            verify(mockResource1, atLeast(1)).get();
            verify(mockResource1, never()).patch(any(), any());
            verify(mockResource1, never()).create();
            verify(mockDeletable1, times(1)).delete();

            verify(mockResource2, times(1)).get();
            verify(mockResource2, times(1)).patch(any(), eq(resource2Mod));
            verify(mockResource2, never()).create();
            verify(mockResource2, never()).delete();

            verify(mockResource3, times(1)).get();
            verify(mockResource3, never()).patch(any(), any());
            verify(mockResource3, times(1)).create();
            verify(mockResource3, never()).delete();

            async.flag();
        })));
    }
}
