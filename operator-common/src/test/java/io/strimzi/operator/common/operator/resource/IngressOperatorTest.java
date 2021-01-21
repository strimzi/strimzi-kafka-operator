/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.extensions.IngressBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
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
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
        when(mockExt.ingresses()).thenReturn(op);
        when(mockClient.extensions()).thenReturn(mockExt);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, Ingress, IngressList, Resource<Ingress>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new IngressOperator(vertx, mockClient);
    }
}
