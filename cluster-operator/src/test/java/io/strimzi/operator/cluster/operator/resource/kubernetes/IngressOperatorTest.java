/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.V1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperatorTest;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IngressOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, Ingress, IngressList, Resource<Ingress>> {

    @Override
    protected boolean alwaysUsesServerSideApply() {
        return true;
    }

    @Override
    protected boolean supportsServerSideApply() {
        return alwaysUsesServerSideApply();
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
    protected Ingress resource(String name) {
        return new IngressBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .build();
    }

    @Override
    protected Ingress modifiedResource(String name) {
        return new IngressBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<Ingress, IngressList, Resource<Ingress>> op) {
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        V1NetworkAPIGroupDSL v1 = mock(V1NetworkAPIGroupDSL.class);
        when(network.v1()).thenReturn(v1);
        when(v1.ingresses()).thenReturn(op);
        when(mockClient.network()).thenReturn(network);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, Ingress, IngressList, Resource<Ingress>> createResourceOperations(KubernetesClient mockClient) {
        return new IngressOperator(asyncExecutor, mockClient);
    }
}
