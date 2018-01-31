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

import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatefulSetResourcesTest extends ResourceOperationTest {

    @Test
    public void testStatefulSetCreation(TestContext context) {
        StatefulSet dep = new StatefulSetBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
            when(mockExt.statefulSets()).thenReturn(mockCms);
            when(mockClient.apps()).thenReturn(mockExt);
        };
        BiFunction<Vertx, KubernetesClient, StatefulSetResources> f = (vertx1, client) -> new StatefulSetResources(vertx1, client);

        createWhenExistsIsANop(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, f);
        existenceCheckThrows(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, f);
        successfulCreation(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, f);
        creationThrows(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, f);
    }
    @Test
    public void testStatefulSetDeletion(TestContext context) {
        StatefulSet dep = new StatefulSetBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiFunction<Vertx, KubernetesClient, StatefulSetResources> f = (vertx1, client) -> new StatefulSetResources(vertx1, client);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
            when(mockExt.statefulSets()).thenReturn(mockCms);
            when(mockClient.apps()).thenReturn(mockExt);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                dep,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                dep,
                mocker,
                f);
        successfulDeletion(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                dep,
                mocker,
                f);
        deletionThrows(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                dep,
                mocker,
                f);
    }
}
