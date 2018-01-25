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
import io.strimzi.controller.cluster.MockK8sUtils;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class CreateConfigMapOperationTest {

    private static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateWhenMapExistsIsANop(TestContext context) {
        Async async = context.async();
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName("name")
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
        CreateConfigMapOperation op = new CreateConfigMapOperation(cm);
        op.execute(vertx, new MockK8sUtils() {
            public boolean configMapExists(String namespace, String name) {
                return true;
            }
        }, ar -> {
            assertTrue(ar.succeeded());
            async.complete();
        });
    }

    @Test
    public void testCreateWhenMapExistsThrows(TestContext context) {
        Async async = context.async();
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("name")
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
        RuntimeException ex = new RuntimeException();
        CreateConfigMapOperation op = new CreateConfigMapOperation(cm);
        op.execute(vertx, new MockK8sUtils() {
            public boolean configMapExists(String namespace, String name) {
                throw ex;
            }
        }, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }

    @Test
    public void testCreationOfAConfigMap(TestContext context) {
        Async async = context.async(2);
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("name")
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
        CreateConfigMapOperation op = new CreateConfigMapOperation(cm);
        op.execute(vertx, new MockK8sUtils() {
            public boolean configMapExists(String namespace, String name) {
                return false;
            }
            public void createConfigMap(ConfigMap cm) {
                async.countDown();
            }
        }, ar -> {
            assertTrue(ar.succeeded());
            async.countDown();
        });
    }

    @Test
    public void testCreationOfAConfigMapThrows(TestContext context) {
        Async async = context.async();
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("name")
                .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
        CreateConfigMapOperation op = new CreateConfigMapOperation(cm);
        RuntimeException ex = new RuntimeException();
        op.execute(vertx, new MockK8sUtils() {
            public boolean configMapExists(String namespace, String name) {
                return false;
            }
            public void createConfigMap(ConfigMap cm) {
                throw ex;
            }
        }, ar -> {
            assertTrue(ar.failed());
            assertEquals(ex, ar.cause());
            async.complete();
        });
    }
}
