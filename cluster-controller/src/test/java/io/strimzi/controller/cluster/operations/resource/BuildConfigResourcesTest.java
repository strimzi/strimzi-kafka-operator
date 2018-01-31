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
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.mockito.Mockito.when;

public class BuildConfigResourcesTest extends ResourceOperationTest {

    @Test
    public void testBuildConfigCreation(TestContext context) {
        BuildConfig dep = new BuildConfigBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.buildConfigs()).thenReturn(mockCms);
        BiFunction<Vertx, OpenShiftClient, BuildConfigResources> f = (vertx1, client) -> new BuildConfigResources(vertx1, client);

        createWhenExistsIsANop(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, f);
        existenceCheckThrows(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, f);
        successfulCreation(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, f);
        creationThrows(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, f);
    }

    @Test
    public void testBuildConfigDeletion(TestContext context) {
        BuildConfig dep = new BuildConfigBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiFunction<Vertx, OpenShiftClient, ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>>> f = (vertx1, client) -> new BuildConfigResources(vertx1, client);
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) ->
                when(mockClient.buildConfigs()).thenReturn(mockCms);

        deleteWhenResourceDoesNotExistIsANop(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                dep,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                dep,
                mocker,
                f);
        successfulDeletion(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                dep,
                mocker,
                f);
        deletionThrows(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                dep,
                mocker,
                f);
    }
}
