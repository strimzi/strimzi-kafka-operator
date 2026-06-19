/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.gatewayapi.v1.TLSRoute;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.TLSRouteBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.TLSRouteList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperatorTest;

import static org.mockito.Mockito.when;

public class TLSRouteOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, TLSRoute, TLSRouteList, Resource<TLSRoute>> {
    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    @SuppressWarnings({ "rawtypes" })
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected TLSRoute resource(String name) {
        return new TLSRouteBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withHostnames("my-hostname")
                .endSpec()
                .build();
    }

    @Override
    protected TLSRoute modifiedResource(String name) {
        return new TLSRouteBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withHostnames("my-other-hostname")
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<TLSRoute, TLSRouteList, Resource<TLSRoute>> op) {
        when(mockClient.resources(TLSRoute.class, TLSRouteList.class)).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, TLSRoute, TLSRouteList, Resource<TLSRoute>> createResourceOperations(KubernetesClient mockClient) {
        return new TLSRouteOperator(asyncExecutor, mockClient);
    }
}
