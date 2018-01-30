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
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
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
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.controller.cluster.operations.resource.BuildConfigResources;
import io.strimzi.controller.cluster.operations.resource.ConfigMapResources;
import io.strimzi.controller.cluster.operations.resource.DeploymentResources;
import io.strimzi.controller.cluster.operations.resource.ImageStreamResources;
import io.strimzi.controller.cluster.operations.resource.PvcResources;
import io.strimzi.controller.cluster.operations.resource.ResourceOperation;
import io.strimzi.controller.cluster.operations.resource.ServiceResources;
import io.strimzi.controller.cluster.operations.resource.StatefulSetResources;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

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
public class ResourceOperationTest {

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

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
            void createWhenExistsIsANop(TestContext context,
                    Class<C> clientType,
                    Class<? extends Resource> resourceType,
                    T resource,
                    BiConsumer<C, MixedOperation> mocker,
                    BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).createNew();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            async.complete();
        });
    }


    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
            void existenceCheckThrows(TestContext context,
                      Class<C> clientType,
                      Class<? extends Resource> resourceType,
                      T resource,
                      BiConsumer<C, MixedOperation> mocker,
                      BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
            void successfulCreation(TestContext context,
                    Class<C> clientType,
                    Class<? extends Resource> resourceType,
                    T resource,
                    BiConsumer<C, MixedOperation> mocker,
                    BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockCms).createOrReplace(eq(resource));
            async.complete();
        });
    }

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
            void creationThrows(TestContext context,
                    Class<C> clientType,
                    Class<? extends Resource> resourceType,
                    T resource,
                    BiConsumer<C, MixedOperation> mocker,
                    BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
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

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
            void deleteWhenResourceDoesNotExistIsANop(TestContext context,
                   Class<C> clientType,
                   Class<? extends Resource> resourceType,
                   T resource,
                   BiConsumer<C, MixedOperation> mocker,
                   BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            verify(mockCms, never()).delete();
            async.complete();
        });
    }

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
    void deleteWhenResourceExistsThrows(TestContext context,
                                        Class<C> clientType,
                                        Class<? extends Resource> resourceType,
                                        T resource,
                                        BiConsumer<C, MixedOperation> mocker,
                                        BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
    void successfulDeletion(TestContext context,
                            Class<C> clientType,
                            Class<? extends Resource> resourceType,
                            T resource,
                            BiConsumer<C, MixedOperation> mocker,
                            BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource).delete();
            async.complete();
        });
    }

    private <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R2 extends Resource<T, D>>
    void deletionThrows(TestContext context,
                        Class<C> clientType,
                        Class<? extends Resource> resourceType,
                        T resource,
                        BiConsumer<C, MixedOperation> mocker,
                        BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R2>> f) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.delete()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R2> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

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
    public void testDeploymentCreation(TestContext context) {
        Deployment dep = new DeploymentBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
            when(mockExt.deployments()).thenReturn(mockCms);
            when(mockClient.extensions()).thenReturn(mockExt);
        };
        BiFunction<Vertx, KubernetesClient, DeploymentResources> f = (vertx1, client) -> new DeploymentResources(vertx1, client);

        createWhenExistsIsANop(context, KubernetesClient.class, ScalableResource.class, dep, mocker, f);
        existenceCheckThrows(context, KubernetesClient.class, ScalableResource.class, dep, mocker, f);
        successfulCreation(context, KubernetesClient.class, ScalableResource.class, dep, mocker, f);
        creationThrows(context, KubernetesClient.class, ScalableResource.class, dep, mocker, f);
    }

    @Test
    public void testServiceCreation(TestContext context) {
        Service dep = new ServiceBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.services()).thenReturn(mockCms);

        BiFunction<Vertx, KubernetesClient, ServiceResources> f = (vertx1, client) -> new ServiceResources(vertx1, client);

        createWhenExistsIsANop(context, KubernetesClient.class, Resource.class, dep, mocker, f);
        existenceCheckThrows(context, KubernetesClient.class, Resource.class, dep, mocker, f);
        successfulCreation(context, KubernetesClient.class, Resource.class, dep, mocker, f);
        creationThrows(context, KubernetesClient.class, Resource.class, dep, mocker, f);
    }

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

    @Test
    public void testDeploymentDeletion(TestContext context) {
        Deployment dep = new DeploymentBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
            when(mockExt.deployments()).thenReturn(mockCms);
            when(mockClient.extensions()).thenReturn(mockExt);
        };

        BiFunction<Vertx, KubernetesClient, DeploymentResources> f = (vertx1, client) -> new DeploymentResources(vertx1, client);

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
        successfulDeletion(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
        deletionThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
    }

    @Test
    public void testServiceDeletion(TestContext context) {
        Service dep = new ServiceBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiFunction<Vertx, KubernetesClient, ServiceResources> f = (vertx1, client) -> new ServiceResources(vertx1, client);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            when(mockClient.services()).thenReturn(mockCms);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
        successfulDeletion(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
        deletionThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                dep,
                mocker,
                f);
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

    @Test
    public void testPvcDeletion(TestContext context) {
        PersistentVolumeClaim dep = new PersistentVolumeClaimBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        BiFunction<Vertx, KubernetesClient, PvcResources> f = (vertx1, client) -> new PvcResources(vertx1, client);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            when(mockClient.persistentVolumeClaims()).thenReturn(mockCms);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                Resource.class,
                dep,
                mocker,
                f);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                Resource.class,
                dep,
                mocker,
                f);
        successfulDeletion(context,
                KubernetesClient.class,
                Resource.class,
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
