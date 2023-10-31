/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.GracePeriodConfigurable;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PodOperatorServerSideApplyTest extends
        AbstractReadyResourceOperatorTest<KubernetesClient, Pod, PodList, PodResource> {
    @Override
    protected Class clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Pod resource(String name) {
        return new PodBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withHostname("foo")
                .endSpec()
                .build();
    }

    @Override
    protected Pod modifiedResource(String name) {
        return new PodBuilder(resource(name))
                .editSpec()
                    .withHostname("bar")
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient client, MixedOperation op) {
        when(client.pods()).thenReturn(op);
    }

    @Override
    protected PodOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodOperator(vertx, mockClient, true);
    }

    @Override
    @Test
    public void testSuccessfulCreation(VertxTestContext context) {
        //this is rewritten as using ServerSideApply the get and create will no longer be called
        Pod resource = resource();
        Resource mockResource = mock(resourceType());

        when(mockResource.get()).thenReturn(null);
        when(mockResource.patch(any(PatchContext.class), any(Pod.class))).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);
        when(mockNameable.resource(eq(resource))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<KubernetesClient, Pod, PodList, PodResource> op = createResourceOperationsWithMockedReadiness(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).patch(any(PatchContext.class), any(Pod.class));
            async.flag();
        })));
    }

    @Override
    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        //this is rewritten as using ServerSideApply the get will no longer be called
        Pod resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);

        when(mockResource.patch(any(), (Pod) any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<KubernetesClient, Pod, PodList, PodResource> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, modifiedResource()).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).patch(any(PatchContext.class), any(Pod.class));
            verify(mockResource, never()).create();
            async.flag();
        })));
    }

    @Override
    @Disabled
    public void testCreateOrUpdateThrowsWhenCreateThrows(VertxTestContext context) {
        //not valid as we no longer perform the create (this is done as a patch)
    }

    @Override
    @Disabled
    public void testExistenceCheckThrows(VertxTestContext context) {
        //not valid as we no longer perform the get (the get is only done prior to delete)
    }

    @Test
    public void testDeleteWhenResourceDoesNotExistIsANop(VertxTestContext context) {
        //this is rewritten as using ServerSideApply the delete will now be called whenever desired is null
        Deletable mockDeletable = mock(Deletable.class);
        when(mockDeletable.delete()).thenReturn(List.of());
        GracePeriodConfigurable mockDeletableGrace = mock(GracePeriodConfigurable.class);
        when(mockDeletableGrace.delete()).thenReturn(List.of());

        Pod resource = resource();
        Resource mockResource = mock(resourceType());
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

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<KubernetesClient, Pod, PodList, PodResource> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
                .onComplete(context.succeeding(rr -> context.verify(() -> {
                    verify(mockDeletable).delete();
                    async.flag();
                })));
    }

    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context) {
        //this is rewritten as using ServerSideApply the patch will now be called whenever desired is not null
        Pod resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);
        when(mockResource.patch(any(PatchContext.class), any(Pod.class))).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<KubernetesClient, Pod, PodList, PodResource> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource()).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource, never()).get();
            verify(mockResource).patch(any(PatchContext.class), any(Pod.class));
            verify(mockResource, never()).create();
            async.flag();
        })));
    }

    @Test
    public void testBatchReconciliation(VertxTestContext context) {
        Map<String, String> selector = Map.of("labelA", "a", "labelB", "b");

        Pod resource1 = resource("resource-1");
        Pod resource2 = resource("resource-2");
        Pod resource2Mod = modifiedResource("resource-2");
        Pod resource3 = resource("resource-3");

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
        when(mockResource2.patch(any(PatchContext.class), eq(resource2Mod))).thenReturn(resource2Mod);

        Resource mockResource3 = mock(resourceType());
        when(mockResource3.get()).thenReturn(null);
        when(mockResource3.patch(any(PatchContext.class), eq(resource3))).thenReturn(resource3);

        KubernetesResourceList mockResourceList = mock(KubernetesResourceList.class);
        when(mockResourceList.getItems()).thenReturn(List.of(resource1, resource2));

        FilterWatchListDeletable mockListable = mock(FilterWatchListDeletable.class);
        when(mockListable.list(any())).thenReturn(mockResourceList);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withLabels(eq(selector))).thenReturn(mockListable);
        when(mockNameable.withName(eq("resource-1"))).thenReturn(mockResource1);
        when(mockNameable.withName(eq("resource-2"))).thenReturn(mockResource2);
        when(mockNameable.withName(eq("resource-3"))).thenReturn(mockResource3);
        when(mockNameable.resource(eq(resource3))).thenReturn(mockResource3);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(anyString())).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        mocker(mockClient, mockCms);

        AbstractNamespacedResourceOperator<KubernetesClient, Pod, PodList, PodResource> op = createResourceOperations(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.batchReconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, List.of(resource2Mod, resource3), Labels.fromMap(selector)).onComplete(context.succeeding(i -> context.verify(() -> {
            verify(mockResource1, atLeast(1)).get();
            verify(mockResource1, never()).patch(any(), any());
            verify(mockResource1, never()).create();
            verify(mockDeletable1, times(1)).delete();

            verify(mockResource2, times(1)).patch(any(), eq(resource2Mod));
            verify(mockResource2, never()).create();
            verify(mockResource2, never()).delete();

            verify(mockResource3, never()).patch(any(), any());
            verify(mockResource3, times(1)).patch(any(), eq(resource3));
            verify(mockResource3, never()).delete();

            async.flag();
        })));
    }
}
