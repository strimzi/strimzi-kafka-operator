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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.controller.cluster.operations.DeleteOperation;
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
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DeleteOperationTest {

    public static final String NAME = "name";
    public static final String NAMESPACE = "namespace";
    private static Vertx vertx;

    private final ConfigMap cm = new ConfigMapBuilder()
            .withNewMetadata()
            .withName(NAME)
            .withNamespace(NAMESPACE)
            .withLabels(singletonMap("foo", "bar"))
            .endMetadata()
            .withData(singletonMap("FOO", "BAR"))
            .build();

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    private <C extends KubernetesClient> void deleteWhenResourceDoesNotExistIsANop(TestContext context,
                                                       Class<C> clientType,
                                                       Class<? extends Resource> resourceType,
                                                       BiConsumer<C, MixedOperation> mocker,
                                                       DeleteOperation<C> op) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            verify(mockCms, never()).delete();
            async.complete();
        });
    }

    private <C extends KubernetesClient> void deleteWhenResourceExistsThrows(TestContext context,
                                                                 Class<C> clientType,
                                                                 Class<? extends Resource> resourceType,
                                                                 BiConsumer<C, MixedOperation> mocker,
                                                                 DeleteOperation<C> op) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    private <C extends KubernetesClient> void successfulDeletion(TestContext context,
                                    Class<C> clientType,
                                    Class<? extends Resource> resourceType,
                                    BiConsumer<C, MixedOperation> mocker,
                                    DeleteOperation<C> op) {
        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(cm);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType);
        mocker.accept(mockClient, mockCms);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource).delete();
            async.complete();
        });
    }

    private <C extends KubernetesClient> void deletionThrows(TestContext context,
                                Class<C> clientType,
                                Class<? extends Resource> resourceType,
                                BiConsumer<C, MixedOperation> mocker,
                                DeleteOperation<C> op) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType);
        when(mockResource.get()).thenReturn(cm);
        when(mockResource.delete()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

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

        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteConfigMap(NAMESPACE, NAME);

        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> when(mockClient.configMaps()).thenReturn(mockCms);
        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
        successfulDeletion(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
        deletionThrows(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
    }

    @Test
    public void testDeployment(TestContext context) {

        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteDeployment(NAMESPACE, NAME);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
            when(mockExt.deployments()).thenReturn(mockCms);
            when(mockClient.extensions()).thenReturn(mockExt);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
        successfulDeletion(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
        deletionThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
    }

    @Test
    public void testService(TestContext context) {

        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteService(NAMESPACE, NAME);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            when(mockClient.services()).thenReturn(mockCms);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
        successfulDeletion(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
        deletionThrows(context,
                KubernetesClient.class,
                ScalableResource.class,
                mocker,
                op);
    }

    @Test
    public void testStatefulSet(TestContext context) {

        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteStatefulSet(NAMESPACE, NAME);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
            when(mockExt.statefulSets()).thenReturn(mockCms);
            when(mockClient.apps()).thenReturn(mockExt);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                mocker,
                op);
        successfulDeletion(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                mocker,
                op);
        deletionThrows(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                mocker,
                op);
    }

    @Test
    public void testPvc(TestContext context) {

        DeleteOperation<KubernetesClient> op = DeleteOperation.deletePersistentVolumeClaim(NAMESPACE, NAME);
        BiConsumer<KubernetesClient, MixedOperation> mocker = (mockClient, mockCms) -> {
            when(mockClient.persistentVolumeClaims()).thenReturn(mockCms);
        };

        deleteWhenResourceDoesNotExistIsANop(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
        successfulDeletion(context,
                KubernetesClient.class,
                Resource.class,
                mocker,
                op);
        deletionThrows(context,
                KubernetesClient.class,
                RollableScalableResource.class,
                mocker,
                op);
    }

    @Test
    public void testBuildConfig(TestContext context) {

        DeleteOperation<OpenShiftClient> op = DeleteOperation.deleteBuildConfig(NAMESPACE, NAME);
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) ->
                when(mockClient.buildConfigs()).thenReturn(mockCms);

        deleteWhenResourceDoesNotExistIsANop(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                mocker,
                op);
        successfulDeletion(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                mocker,
                op);
        deletionThrows(context,
                OpenShiftClient.class,
                BuildConfigResource.class,
                mocker,
                op);
    }

    @Test
    public void testImageStream(TestContext context) {

        DeleteOperation<OpenShiftClient> op = DeleteOperation.deleteImageStream(NAMESPACE, NAME);
        BiConsumer<OpenShiftClient, MixedOperation> mocker = (mockClient, mockCms) ->
                when(mockClient.imageStreams()).thenReturn(mockCms);

        deleteWhenResourceDoesNotExistIsANop(context,
                OpenShiftClient.class,
                Resource.class,
                mocker,
                op);
        deleteWhenResourceExistsThrows(context,
                OpenShiftClient.class,
                Resource.class,
                mocker,
                op);
        successfulDeletion(context,
                OpenShiftClient.class,
                Resource.class,
                mocker,
                op);
        deletionThrows(context,
                OpenShiftClient.class,
                Resource.class,
                mocker,
                op);
    }
}
