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

package io.strimzi.controller.cluster.operations.kubernetes;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.controller.cluster.operations.CreateOperation;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.BiConsumer;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class CreateOperationTest {

    public static final String NAME = "name";
    public static final String NAMESPACE = "namespace";
    private static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    private <C extends KubernetesClient, R extends HasMetadata> void createWhenExistsIsANop(TestContext context,
                                                                Class<C> clientType,
                                                                Class<? extends Resource> resourceType,
                                                                R resource,
                                                                BiConsumer<C, MixedOperation> mocker,
                                                                CreateOperation<C, R> op) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).createNew();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            async.complete();
        });
    }


    private <C extends KubernetesClient, R extends HasMetadata> void existenceCheckThrows(TestContext context,
                                      Class<C> clientType,
                                      Class<? extends Resource> resourceType,
                                      R resource,
                                      BiConsumer<C, MixedOperation> mocker,
                                      CreateOperation<C, R> op) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    private <C extends KubernetesClient, R extends HasMetadata> void successfulCreation(TestContext context,
                                                            Class<C> clientType,
                                                            Class<? extends Resource> resourceType,
                                                            R resource,
                                                            BiConsumer<C, MixedOperation> mocker,
                                                            CreateOperation<C, R> op) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockCms).createOrReplace(eq(resource));
            async.complete();
        });
    }

    private <C extends KubernetesClient, R extends HasMetadata> void creationThrows(TestContext context,
                                                        Class<C> clientType,
                                                        Class<? extends Resource> resourceType,
                                                        R resource,
                                                        BiConsumer<C, MixedOperation> mocker,
                                                        CreateOperation<C, R> op) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);
        when(mockCms.createOrReplace(any())).thenThrow(ex);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();

        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    @Test
    public void testConfigMap(TestContext context) {
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();

        CreateOperation<KubernetesClient, ConfigMap> op = CreateOperation.createConfigMap(cm);

        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.configMaps()).thenReturn(mockCms);

        createWhenExistsIsANop(context, KubernetesClient.class, Resource.class, cm, mocker, op);
        existenceCheckThrows(context, KubernetesClient.class, Resource.class, cm, mocker, op);
        successfulCreation(context, KubernetesClient.class, Resource.class, cm, mocker, op);
        creationThrows(context, KubernetesClient.class, Resource.class, cm, mocker, op);
    }

    @Test
    public void testDeployment(TestContext context) {
        Deployment dep = new DeploymentBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
            when(mockExt.deployments()).thenReturn(mockCms);
            when(mockClient.extensions()).thenReturn(mockExt);
        };
        CreateOperation<KubernetesClient, Deployment> op = CreateOperation.createDeployment(dep);

        createWhenExistsIsANop(context, KubernetesClient.class, ScalableResource.class, dep, mocker, op);
        existenceCheckThrows(context, KubernetesClient.class, ScalableResource.class, dep, mocker, op);
        successfulCreation(context, KubernetesClient.class, ScalableResource.class, dep, mocker, op);
        creationThrows(context, KubernetesClient.class, ScalableResource.class, dep, mocker, op);
    }

    @Test
    public void testService(TestContext context) {
        Service dep = new ServiceBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.services()).thenReturn(mockCms);
        CreateOperation<KubernetesClient, Service> op = CreateOperation.createService(dep);

        createWhenExistsIsANop(context, KubernetesClient.class, Resource.class, dep, mocker, op);
        existenceCheckThrows(context, KubernetesClient.class, Resource.class, dep, mocker, op);
        successfulCreation(context, KubernetesClient.class, Resource.class, dep, mocker, op);
        creationThrows(context, KubernetesClient.class, Resource.class, dep, mocker, op);
    }

    @Test
    public void testStatefulSet(TestContext context) {
        StatefulSet dep = new StatefulSetBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
            when(mockExt.statefulSets()).thenReturn(mockCms);
            when(mockClient.apps()).thenReturn(mockExt);
        };
        CreateOperation<KubernetesClient, StatefulSet> op = CreateOperation.createStatefulSet(dep);

        createWhenExistsIsANop(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, op);
        existenceCheckThrows(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, op);
        successfulCreation(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, op);
        creationThrows(context, KubernetesClient.class, RollableScalableResource.class, dep, mocker, op);
    }

    @Test
    public void testBuildConfig(TestContext context) {
        BuildConfig dep = new BuildConfigBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.buildConfigs()).thenReturn(mockCms);
        CreateOperation<OpenShiftClient, BuildConfig> op = CreateOperation.createBuildConfig(dep);

        createWhenExistsIsANop(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, op);
        existenceCheckThrows(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, op);
        successfulCreation(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, op);
        creationThrows(context, OpenShiftClient.class, BuildConfigResource.class, dep, mocker, op);
    }

    @Test
    public void testImageStream(TestContext context) {
        ImageStream dep = new ImageStreamBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.imageStreams()).thenReturn(mockCms);
        CreateOperation<OpenShiftClient, ImageStream> op = CreateOperation.createImageStream(dep);

        createWhenExistsIsANop(context, OpenShiftClient.class, Resource.class, dep, mocker, op);
        existenceCheckThrows(context, OpenShiftClient.class, Resource.class, dep, mocker, op);
        successfulCreation(context, OpenShiftClient.class, Resource.class, dep, mocker, op);
        creationThrows(context, OpenShiftClient.class, Resource.class, dep, mocker, op);
    }
}
