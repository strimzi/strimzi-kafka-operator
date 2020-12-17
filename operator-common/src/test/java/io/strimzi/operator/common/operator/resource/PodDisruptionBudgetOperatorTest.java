/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PolicyAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PodDisruptionBudgetOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> {

    @Override
    protected void  mocker(KubernetesClient mockClient, MixedOperation op) {
        PolicyAPIGroupDSL mockPolicy = mock(PolicyAPIGroupDSL.class);
        when(mockPolicy.podDisruptionBudget()).thenReturn(op);
        when(mockClient.policy()).thenReturn(mockPolicy);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodDisruptionBudgetOperator(vertx, mockClient);
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected PodDisruptionBudget resource() {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();
    }

    @Test
    public void testCreateWhenExistsIsAPatch(VertxTestContext context) {
        PodDisruptionBudget existingResource = resource();
        PodDisruptionBudget desiredResource = resource();
        desiredResource.getSpec().setMaxUnavailable(new IntOrString(2));

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(existingResource);
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);
        when(mockResource.patch(any())).thenReturn(existingResource);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, existingResource);
            return (Watch) () -> {

            };
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(existingResource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(existingResource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        Checkpoint async = context.checkpoint();
        createResourceOperations(vertx, mockClient).createOrUpdate(desiredResource).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource).patch(any());
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).create();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            async.flag();
        })));
    }

    @Test
    public void testCreateWhenExistsAndNoDiffIsNotAPatch(VertxTestContext context) {
        PodDisruptionBudget existingResource = resource();
        PodDisruptionBudget desiredResource = resource();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(existingResource);
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);
        when(mockResource.patch(any())).thenReturn(existingResource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(existingResource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(existingResource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        Checkpoint async = context.checkpoint();
        createResourceOperations(vertx, mockClient).createOrUpdate(desiredResource).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource, never()).patch(any());
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).create();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            async.flag();
        })));
    }
}
