/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressList;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLSBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.V1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IngressOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, Ingress, IngressList, Resource<Ingress>> {
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
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        V1NetworkAPIGroupDSL v1 = mock(V1NetworkAPIGroupDSL.class);
        when(network.v1()).thenReturn(v1);
        when(v1.ingresses()).thenReturn(op);
        when(mockClient.network()).thenReturn(network);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, Ingress, IngressList, Resource<Ingress>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new IngressOperator(vertx, mockClient);
    }

    @Test
    public void testIngressClassPatching()  {
        KubernetesClient client = mock(KubernetesClient.class);

        Ingress current = new IngressBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withIngressClassName("nginx")
                    .withTls(new IngressTLSBuilder().withHosts("my-host").build())
                .endSpec()
                .build();

        Ingress desired = new IngressBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withIngressClassName(null)
                    .withTls(new IngressTLSBuilder().withHosts("my-host").build())
                .endSpec()
                .build();

        IngressOperator op = new IngressOperator(vertx, client);
        op.patchIngressClassName(current, desired);

        assertThat(desired.getSpec().getIngressClassName(), is(current.getSpec().getIngressClassName()));
    }
}
