/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> {


    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected ServiceAccount resource(String name) {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
            .build();
    }

    @Override
    protected ServiceAccount modifiedResource(String name) {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.serviceAccounts()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new ServiceAccountOperator(vertx, mockClient);
    }

    @Override
    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        testCreateWhenExistsWithChangeIsAPatch(context, true);
    }

    @Override
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context, boolean cascade) {
        // This is overridden because SA patch is coded as a no op to avoid needless token creation.
        ServiceAccount resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(cascade ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN)).thenReturn(mockResource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        ServiceAccountOperator op = new ServiceAccountOperator(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource)
            .onComplete(context.succeeding(rr -> {
                context.verify(() -> assertThat(rr, instanceOf(ReconcileResult.Noop.class)));
                verify(mockResource).get();
                verify(mockResource, never()).patch(any(), any());
                verify(mockResource, never()).create();
                verify(mockResource, never()).create();
                verify(mockResource, never()).createOrReplace();
                //verify(mockCms, never()).createOrReplace(any());
                async.flag();
            }));
    }

    @Test
    public void testSecretsPatching(VertxTestContext context)   {
        List<ObjectReference> secrets = List.of(
                new ObjectReferenceBuilder().withName("secretName1").build(),
                new ObjectReferenceBuilder().withName("secretName2").build()
        );

        List<LocalObjectReference> imagePullSecrets = List.of(
                new LocalObjectReferenceBuilder().withName("pullSecretName1").build(),
                new LocalObjectReferenceBuilder().withName("pullSecretName2").build()
        );

        ServiceAccount current = new ServiceAccountBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withSecrets(secrets)
                .withImagePullSecrets(imagePullSecrets)
                .build();

        ServiceAccount desired = new ServiceAccountBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                    .withLabels(Map.of("lKey", "lValue"))
                    .withAnnotations(Map.of("aKey", "aValue"))
                .endMetadata()
                .build();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(current);
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockResource.patch(any(), saCaptor.capture())).thenReturn(desired);
        when(mockResource.withPropagationPolicy(DeletionPropagation.FOREGROUND)).thenReturn(mockResource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        ServiceAccountOperator op = new ServiceAccountOperator(vertx, mockClient);

        Checkpoint async = context.checkpoint();
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, desired)
                .onComplete(context.succeeding(rr -> {
                    verify(mockResource, times(1)).patch(any(), any(ServiceAccount.class));

                    assertThat(saCaptor.getValue(), is(notNullValue()));
                    assertThat(saCaptor.getValue().getSecrets().size(), is(2));
                    assertThat(saCaptor.getValue().getSecrets(), is(secrets));
                    assertThat(saCaptor.getValue().getImagePullSecrets().size(), is(2));
                    assertThat(saCaptor.getValue().getImagePullSecrets(), is(imagePullSecrets));
                    assertThat(saCaptor.getValue().getMetadata().getLabels().get("lKey"), is("lValue"));
                    assertThat(saCaptor.getValue().getMetadata().getAnnotations().get("aKey"), is("aValue"));

                    async.flag();
                }));
    }
}
