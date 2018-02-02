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
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.server.mock.OpenShiftServer;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import okhttp3.mockwebserver.MockResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
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

    Map<String,String> labels(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i+=2) {
            map.put(pairs[i], pairs[i+1]);
        }
        return map;
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
        Map<String, String> cmData = new HashMap<>();
        int replicas = 1;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        cmData.put(KafkaCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaCluster.KEY_IMAGE, image);
        cmData.put(KafkaCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaCluster.KEY_STORAGE, "{\"type\": \"ephemeral\"}");
        ConfigMap clusterCm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterCmName)
                    .withNamespace(clusterCmNamespace)
                    .withLabels(singletonMap("strimzi.io/kind", "kafka-cluster"))
                .endMetadata()
                .withData(cmData)
                .build();
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
            // Assertions about the CM, Services and SS

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).create(any());

            // We expect a headless and headful service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(2, capturedServices.size());
            int headfulIndex = (clusterCmName+"-kafka").equals(capturedServices.get(0).getMetadata().getName()) ? 0 : 1;

            // Assertions on the external service
            Service headful = capturedServices.get(headfulIndex);
            context.assertEquals("ClusterIP", headful.getSpec().getType());
            context.assertEquals(labels("strimzi.io/cluster", "foo", "strimzi.io/kind", "kafka-cluster", "strimzi.io/name", "foo-kafka"), headful.getSpec().getSelector());
            context.assertEquals(1, headful.getSpec().getPorts().size());
            context.assertEquals("clients", headful.getSpec().getPorts().get(0).getName());
            context.assertEquals(9092, headful.getSpec().getPorts().get(0).getPort());
            context.assertEquals("TCP", headful.getSpec().getPorts().get(0).getProtocol());

            // Assertions on the headless service
            Service headless = capturedServices.get(1 - headfulIndex);
            context.assertEquals(clusterCmName+"-kafka-headless", headless.getMetadata().getName());
            context.assertEquals("ClusterIP", headless.getSpec().getType());
            context.assertEquals("None", headless.getSpec().getClusterIP());
            context.assertEquals(labels("strimzi.io/cluster", "foo", "strimzi.io/kind", "kafka-cluster", "strimzi.io/name", "foo-kafka"), headless.getSpec().getSelector());
            context.assertEquals(1, headless.getSpec().getPorts().size());
            context.assertEquals("clients", headless.getSpec().getPorts().get(0).getName());
            context.assertEquals(9092, headless.getSpec().getPorts().get(0).getPort());
            context.assertEquals("TCP", headless.getSpec().getPorts().get(0).getProtocol());

            // Assertions on the statefulset
            List<StatefulSet> capturedSs = ssCaptor.getAllValues();
            // We expect a single statefulSet ...
            context.assertEquals(1, capturedSs.size());
            // ... for the kafka cluster ...
            context.assertEquals(clusterCmName + "-kafka", capturedSs.get(0).getMetadata().getName());
            // ... in the same namespace ...
            context.assertEquals(clusterCmNamespace, capturedSs.get(0).getMetadata().getNamespace());
            // ... with these labels
            context.assertEquals(labels("strimzi.io/cluster", clusterCmName,
                            "strimzi.io/kind", "kafka-cluster",
                            "strimzi.io/name", clusterCmName + "-kafka"),
                    capturedSs.get(0).getMetadata().getLabels());

            context.assertEquals(replicas, capturedSs.get(0).getSpec().getReplicas());
            context.assertEquals(image, capturedSs.get(0).getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            context.assertEquals(healthTimeout, capturedSs.get(0).getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds());
            context.assertEquals(healthDelay, capturedSs.get(0).getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds());
            context.assertEquals(healthTimeout, capturedSs.get(0).getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());
            context.assertEquals(healthDelay, capturedSs.get(0).getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
            // PvcOperations only used for deletion
            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }


}
