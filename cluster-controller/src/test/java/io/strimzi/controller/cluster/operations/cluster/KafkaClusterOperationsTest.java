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

package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaClusterOperationsTest {

    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(TestContext context) {
        createCluster(context, true);
    }

    private void createCluster(TestContext context, boolean isOpenShift) {
        // create CM, Service, headless service, statefulset
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);

        // Create a CM
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        int replicas = 1;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        ConfigMap clusterCm = ResourceUtils.createConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout);
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.create(serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StatefulSet> ssCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockSsOps.create(ssCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, isOpenShift,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps);

        // Now try to create a KafkaCluster based on this CM
        Async async = context.async();
        ops.create(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).create(any());

            // We expect a headless and headful service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(2, capturedServices.size());
            int headfulIndex = (clusterCmName+"-kafka").equals(capturedServices.get(0).getMetadata().getName()) ? 0 : 1;
            // The other service should be the headless one
            Service headless = capturedServices.get(1 - headfulIndex);
            context.assertEquals(clusterCmName+"-kafka-headless", headless.getMetadata().getName());

            // Assertions on the statefulset
            List<StatefulSet> capturedSs = ssCaptor.getAllValues();
            // We expect a single statefulSet ...
            context.assertEquals(1, capturedSs.size());

            // PvcOperations only used for deletion
            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }

}
