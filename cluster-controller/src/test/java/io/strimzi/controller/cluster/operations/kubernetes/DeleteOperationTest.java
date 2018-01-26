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
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.controller.cluster.operations.DeleteOperation;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

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
    
    @Test
    public void testDeleteWhenMapDoesNotExistIsANop(TestContext context) {
        Resource mockResource = mock(Resource.class);
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockClient.configMaps()).thenReturn(mockCms);

        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteConfigMap(NAMESPACE, NAME);

        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource, never()).delete();
            verify(mockCms, never()).delete();
            async.complete();
        });
    }

    @Test
    public void testDeleteWhenMapExistsThrows(TestContext context) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(Resource.class);
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockClient.configMaps()).thenReturn(mockCms);

        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteConfigMap(NAMESPACE, NAME);
        Async async = context.async();
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    @Test
    public void testDeletionOfAConfigMap(TestContext context) {
        Resource mockResource = mock(Resource.class);
        when(mockResource.get()).thenReturn(cm);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockClient.configMaps()).thenReturn(mockCms);

        Async async = context.async();
        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteConfigMap(NAMESPACE, NAME);
        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.succeeded());
            verify(mockResource).get();
            verify(mockResource).delete();
            async.complete();
        });
    }

    @Test
    public void testDeletionOfAConfigMapThrows(TestContext context) {
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(Resource.class);
        when(mockResource.get()).thenReturn(cm);
        when(mockResource.delete()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockClient.configMaps()).thenReturn(mockCms);

        Async async = context.async();
        DeleteOperation<KubernetesClient> op = DeleteOperation.deleteConfigMap(NAMESPACE, NAME);

        op.execute(vertx, mockClient, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }
}
