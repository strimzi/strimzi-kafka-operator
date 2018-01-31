/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;

public class ConfigMapResourcesTest extends ResourceOperationTest {
    @Test
    public void testConfigMapCreation(TestContext context) {
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();

        BiFunction<Vertx, KubernetesClient, ConfigMapResources> f = (vertx1, client) -> new ConfigMapResources(vertx1, client);

        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.configMaps()).thenReturn(mockCms);

        createWhenExistsIsANop(context, KubernetesClient.class, Resource.class, cm, mocker, f);
        existenceCheckThrows(context, KubernetesClient.class, Resource.class, cm, mocker, f);
        successfulCreation(context, KubernetesClient.class, Resource.class, cm, mocker, f);
        creationThrows(context, KubernetesClient.class, Resource.class, cm, mocker, f);
    }

    @Test
    public void testConfigMapDeletion(TestContext context) {
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();

        BiFunction<Vertx, KubernetesClient, ConfigMapResources> f = (vertx1, client) -> new ConfigMapResources(vertx1, client);

        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.configMaps()).thenReturn(mockCms);
        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                Resource.class,
                cm,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                Resource.class,
                cm,
                mocker,
                f);
        successfulDeletion(context,
                KubernetesClient.class,
                Resource.class,
                cm,
                mocker,
                f);
        deletionThrows(context,
                KubernetesClient.class,
                Resource.class,
                cm,
                mocker,
                f);
    }
}
