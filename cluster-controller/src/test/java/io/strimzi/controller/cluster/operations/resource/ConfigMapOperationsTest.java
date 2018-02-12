/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.cluster.operations.resource;

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

public class ConfigMapOperationsTest extends ResourceOperationsMockTest<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> {

    protected void  mocker(KubernetesClient mockClient, MixedOperation mockCms) {
        when(mockClient.configMaps()).thenReturn(mockCms);
    }

    @Override
    protected AbstractOperations<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new ConfigMapOperations(vertx, mockClient);
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
