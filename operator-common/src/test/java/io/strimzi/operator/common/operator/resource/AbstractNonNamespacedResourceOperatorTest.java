/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public abstract class AbstractNonNamespacedResourceOperatorTest<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList, D, R extends Resource<T, D>> {

    public static final String RESOURCE_NAME = "my-resource";
    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
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
     * Configure the given {@code mockClient} to return the given {@code op}
     * that's appropriate for the kind of resource being tests.
     */
    protected abstract void mocker(C mockClient, MixedOperation op);

    /** Create the subclass of ResourceOperation to be tested */
    protected abstract AbstractNonNamespacedResourceOperator<C, T, L, D, R> createResourceOperations(
            Vertx vertx, C mockClient);

    /** Create the subclass of ResourceOperation to be tested with mocked readiness checks*/
    protected AbstractNonNamespacedResourceOperator<C, T, L, D, R> createResourceOperationsWithMockedReadiness(
            Vertx vertx, C mockClient)    {
        return createResourceOperations(vertx, mockClient);
    }

    @Test
    public void createWhenExistsIsAPatch(TestContext context) {
        createWhenExistsIsAPatch(context, true);
    }

    public void createWhenExistsIsAPatch(TestContext context, boolean cascade) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.cascading(cascade)).thenReturn(mockResource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        Future<ReconcileResult<T>> fut = op.createOrUpdate(resource());
        fut.setHandler(ar -> {
            if (!ar.succeeded()) {
                ar.cause().printStackTrace();
            }
            context.assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource).patch(any());
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).createNew();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            async.complete();
        });
    }

    @Test
    public void existenceCheckThrows(TestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.createOrUpdate(resource).setHandler(ar -> {
            context.assertTrue(ar.failed());
            context.assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    @Test
    public void successfulCreation(TestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperationsWithMockedReadiness(
                vertx, mockClient);

        Async async = context.async();
        op.createOrUpdate(resource).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource).create(eq(resource));
            async.complete();
        });
    }

    @Test
    public void creationThrows(TestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);
        when(mockResource.create(any())).thenThrow(ex);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.createOrUpdate(resource).setHandler(ar -> {
            context.assertTrue(ar.failed());
            context.assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    @Test
    public void deletionWhenResourceDoesNotExistIsANop(TestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            async.complete();
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deletionWhenResourceExistsStillDeletes(TestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenReturn(true);
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.assertTrue(ar.succeeded());
            verify(mockResource).delete();
            context.assertTrue(watchWasClosed.get(), "Watch was not closed");
            async.complete();
        });
    }

    @Test
    public void deletionTimesOut(TestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenReturn(true);
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.assertFalse(ar.succeeded());
            context.assertTrue(ar.cause() instanceof TimeoutException, "Got " + ar.cause());
            verify(mockResource).delete();
            context.assertTrue(watchWasClosed.get(), "Watch was not closed");
            async.complete();
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deletionSuccessful(TestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenReturn(Boolean.TRUE);
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            verify(mockResource).delete();
            context.assertTrue(watchWasClosed.get(), "Watch was not closed");
            async.complete();
        });
    }

    @Test
    public void deletion_deleteMethodThrows(TestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.assertTrue(ar.failed());
            context.assertEquals(ex, ar.cause());
            context.assertTrue(watchWasClosed.get(), "Watch was not closed");
            async.complete();
        });
    }

    @Test
    public void deletion_watchMethodThrows(TestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.assertTrue(ar.failed());
            context.assertEquals(ex, ar.cause());
            async.complete();
        });
    }


    @Test
    @SuppressWarnings("unchecked")
    public void deletion_deleteMethodReturnsFalse(TestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenReturn(Boolean.FALSE);
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Async async = context.async();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.assertFalse(ar.succeeded());
            verify(mockResource).delete();
            context.assertTrue(watchWasClosed.get(), "Watch was not closed");
            async.complete();
        });
    }

}

