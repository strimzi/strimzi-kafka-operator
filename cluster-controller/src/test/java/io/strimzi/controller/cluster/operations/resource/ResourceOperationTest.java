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
public abstract class ResourceOperationTest {

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

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
            void createWhenExistsIsANop(TestContext context,
                                        Class<C> clientType,
                                        Class<? extends Resource> resourceType,
                                        T resource,
                                        BiConsumer<C, MixedOperation> mocker,
                                        BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

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


    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
            void existenceCheckThrows(TestContext context,
                                      Class<C> clientType,
                                      Class<? extends Resource> resourceType,
                                      T resource,
                                      BiConsumer<C, MixedOperation> mocker,
                                      BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
            void successfulCreation(TestContext context,
                                    Class<C> clientType,
                                    Class<? extends Resource> resourceType,
                                    T resource,
                                    BiConsumer<C, MixedOperation> mocker,
                                    BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockCms).createOrReplace(eq(resource));
            async.complete();
        });
    }

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
            void creationThrows(TestContext context,
                                Class<C> clientType,
                                Class<? extends Resource> resourceType,
                                T resource,
                                BiConsumer<C, MixedOperation> mocker,
                                BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
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

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.create(resource, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
            void deleteWhenResourceDoesNotExistIsANop(TestContext context,
                                                      Class<C> clientType,
                                                      Class<? extends Resource> resourceType,
                                                      T resource,
                                                      BiConsumer<C, MixedOperation> mocker,
                                                      BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            verify(mockCms, never()).delete();
            async.complete();
        });
    }

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
    void deleteWhenResourceExistsThrows(TestContext context,
                                        Class<C> clientType,
                                        Class<? extends Resource> resourceType,
                                        T resource,
                                        BiConsumer<C, MixedOperation> mocker,
                                        BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
    void successfulDeletion(TestContext context,
                            Class<C> clientType,
                            Class<? extends Resource> resourceType,
                            T resource,
                            BiConsumer<C, MixedOperation> mocker,
                            BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource).delete();
            async.complete();
        });
    }

    protected <C extends KubernetesClient, T extends HasMetadata, L extends KubernetesResourceList, D, R extends Resource<T, D>>
    void deletionThrows(TestContext context,
                        Class<C> clientType,
                        Class<? extends Resource> resourceType,
                        T resource,
                        BiConsumer<C, MixedOperation> mocker,
                        BiFunction<Vertx, C, ? extends ResourceOperation<C, T, L, D, R>> f) {
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

        ResourceOperation<C, T, L, D, R> op = f.apply(vertx, mockClient);

        Async async = context.async();
        op.delete(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }


}

