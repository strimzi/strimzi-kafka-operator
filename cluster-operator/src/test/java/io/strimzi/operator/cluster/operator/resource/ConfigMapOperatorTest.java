/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;

public class ConfigMapOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> {

    @Override
    protected void  mocker(KubernetesClient mockClient, MixedOperation mockCms) {
        when(mockClient.configMaps()).thenReturn(mockCms);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
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
    protected ConfigMap resource() {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(RESOURCE_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
    }
}
