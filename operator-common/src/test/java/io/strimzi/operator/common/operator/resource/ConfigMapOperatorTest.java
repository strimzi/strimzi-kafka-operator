/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;

public class ConfigMapOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, ConfigMap, ConfigMapList, Resource<ConfigMap>> {

    @Override
    protected void  mocker(KubernetesClient mockClient, MixedOperation mockCms) {
        when(mockClient.configMaps()).thenReturn(mockCms);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, ConfigMap, ConfigMapList, Resource<ConfigMap>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new ConfigMapOperator(vertx, mockClient);
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
    protected ConfigMap resource(String name) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
    }

    @Override
    protected ConfigMap modifiedResource(String name) {
        return new ConfigMapBuilder(resource(name))
                .withData(singletonMap("FOO", "BAR2"))
                .build();
    }
}
