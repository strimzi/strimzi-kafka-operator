/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.V1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IngressOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, Ingress, IngressList, Resource<Ingress>> {
    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Ingress resource() {
        return new IngressBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .build();
    }

    @Override
    protected Ingress modifiedResource() {
        return new IngressBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        V1NetworkAPIGroupDSL v1 = mock(V1NetworkAPIGroupDSL.class);
        when(network.v1()).thenReturn(v1);
        when(v1.ingresses()).thenReturn(op);
        when(mockClient.network()).thenReturn(network);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, Ingress, IngressList, Resource<Ingress>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new IngressOperator(vertx, mockClient);
    }
}
