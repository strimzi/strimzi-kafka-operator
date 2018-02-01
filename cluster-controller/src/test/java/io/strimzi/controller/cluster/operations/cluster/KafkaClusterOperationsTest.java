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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.server.mock.OpenShiftServer;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

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

    @Rule
    public OpenShiftServer server = new OpenShiftServer(false, true);

    @Test
    @Ignore
    public void testCreateCluster(TestContext context) {
        KubernetesClient kubeClient = server.getKubernetesClient();
        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, kubeClient,
                new ConfigMapOperations(vertx, kubeClient),
                new ServiceOperations(vertx, kubeClient), new StatefulSetOperations(vertx, kubeClient),
                new PvcOperations(vertx, kubeClient));
        // Create a CM
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaCluster.KEY_REPLICAS, "1");
        ConfigMap clusterCm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterCmName)
                    .withNamespace(clusterCmNamespace)
                    .withLabels(singletonMap("strimzi.io/kind", "kafka-cluster"))
                .endMetadata()
                .withData(cmData)
                .build();
        kubeClient.configMaps().create(clusterCm);

        // Now try to create a KafkaCluster based on this CM
        Async async = context.async();
        ops.create(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());
            async.complete();
        });
    }
}
