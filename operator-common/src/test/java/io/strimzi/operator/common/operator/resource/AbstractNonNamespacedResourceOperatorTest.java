/*
 * Copyright Strimzi authors.
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
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public abstract class AbstractNonNamespacedResourceOperatorTest<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList, D, R extends Resource<T, D>> {

    public static final String RESOURCE_NAME = "my-resource";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
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
    public void createWhenExistsIsAPatch(VertxTestContext context) {
        createWhenExistsIsAPatch(context, true);
    }

    public void createWhenExistsIsAPatch(VertxTestContext context, boolean cascade) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.cascading(cascade)).thenReturn(mockResource);
        when(mockResource.patch(any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        Future<ReconcileResult<T>> fut = op.createOrUpdate(resource());
        fut.setHandler(ar -> {
            if (!ar.succeeded()) {
                ar.cause().printStackTrace();
            }
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            verify(mockResource).get();
            verify(mockResource).patch(any());
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).createNew();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            async.flag();
        });
    }

    @Test
    public void existenceCheckThrows(VertxTestContext context) {
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

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(resource).setHandler(ar -> {
            context.verify(() -> assertThat(ar.failed(), is(true)));
            context.verify(() -> assertThat(ar.cause(), is(ex)));
            async.flag();
        });
    }

    @Test
    public void successfulCreation(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);
        when(mockResource.create(any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperationsWithMockedReadiness(
                vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(resource).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            verify(mockResource).get();
            verify(mockResource).create(eq(resource));
            async.flag();
        });
    }

    @Test
    public void creationThrows(VertxTestContext context) {
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

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(resource).setHandler(ar -> {
            context.verify(() -> assertThat(ar.failed(), is(true)));
            context.verify(() -> assertThat(ar.cause(), is(ex)));
            async.flag();
        });
    }

    @Test
    public void deletionWhenResourceDoesNotExistIsANop(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            async.flag();
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deletionWhenResourceExistsStillDeletes(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
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

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        });
    }

    @Test
    public void deletionTimesOut(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
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

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(false)));
            context.verify(() -> assertThat("Got " + ar.cause(), ar.cause() instanceof TimeoutException, is(true)));
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deletionSuccessful(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
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

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        });
    }

    @Test
    public void deletion_deleteMethodThrows(VertxTestContext context) {
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

        AbstractNonNamespacedResourceOperator<C, T, L, D, R> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.verify(() -> assertThat(ar.failed(), is(true)));
            context.verify(() -> assertThat(ar.cause(), is(ex)));
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        });
    }

    @Test
    public void deletion_watchMethodThrows(VertxTestContext context) {
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

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.verify(() -> assertThat(ar.failed(), is(true)));
            context.verify(() -> assertThat(ar.cause(), is(ex)));
            async.flag();
        });
    }


    @Test
    @SuppressWarnings("unchecked")
    public void deletion_deleteMethodReturnsFalse(VertxTestContext context) {
        T resource = resource();
        AtomicBoolean watchWasClosed = new AtomicBoolean(false);
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withGracePeriod(anyLong())).thenReturn(mockResource);
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

        Checkpoint async = context.checkpoint();
        op.reconcile(resource.getMetadata().getName(), null).setHandler(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(false)));
            verify(mockResource).delete();
            context.verify(() -> assertThat("Watch was not closed", watchWasClosed.get(), is(true)));
            async.flag();
        });
    }

}

