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

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.mockito.Mockito.when;

public class ImageStreamResourcesTest extends ResourceOperationTest {

    @Test
    public void testImageStreamCreation(TestContext context) {
        ImageStream dep = new ImageStreamBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.imageStreams()).thenReturn(mockCms);
        BiFunction<Vertx, OpenShiftClient, ImageStreamResources> f = (vertx1, client) -> new ImageStreamResources(vertx1, client);

        createWhenExistsIsANop(context, OpenShiftClient.class, Resource.class, dep, mocker, f);
        existenceCheckThrows(context, OpenShiftClient.class, Resource.class, dep, mocker, f);
        successfulCreation(context, OpenShiftClient.class, Resource.class, dep, mocker, f);
        creationThrows(context, OpenShiftClient.class, Resource.class, dep, mocker, f);
    }

    @Test
    public void testImageStreamDeletion(TestContext context) {
        ImageStream dep = new ImageStreamBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiFunction<Vertx, OpenShiftClient, ImageStreamResources> f = (vertx1, client) -> new ImageStreamResources(vertx1, client);
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) ->
                when(mockClient.imageStreams()).thenReturn(mockCms);

        deleteWhenResourceDoesNotExistIsANop(context,
                OpenShiftClient.class,
                Resource.class,
                dep,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                OpenShiftClient.class,
                Resource.class,
                dep,
                mocker,
                f);
        successfulDeletion(context,
                OpenShiftClient.class,
                Resource.class,
                dep,
                mocker,
                f);
        deletionThrows(context,
                OpenShiftClient.class,
                Resource.class,
                dep,
                mocker,
                f);
    }
}
