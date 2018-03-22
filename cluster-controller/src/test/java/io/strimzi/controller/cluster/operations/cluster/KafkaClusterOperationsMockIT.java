/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(VertxUnitRunner.class)
public class KafkaClusterOperationsMockIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterOperationsMockIT.class);

    private static final String NAMESPACE = "my-namespace";

    private Vertx vertx;
    private ConfigMap cluster = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("my-cluster")
            .withNamespace(NAMESPACE)
            .withLabels(Labels.forKind("cluster").withType(KafkaClusterOperations.CLUSTER_TYPE_KAFKA).toMap())
            .endMetadata()
            .withData(map(KafkaCluster.KEY_REPLICAS, "7",
                    KafkaCluster.KEY_STORAGE, "{\"type\": \"ephemeral\"}",
                    ZookeeperCluster.KEY_REPLICAS, "3",
                    ZookeeperCluster.KEY_STORAGE, "{\"type\": \"ephemeral\"}"))
            .build();

    @Before
    public void before() {
        this.vertx = Vertx.vertx();
    }

    private static <T> Map<T, T> map(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<T, T> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    // TODO Test with absent storage key
    // TODO Test with storage key lacking a type

    private KafkaClusterOperations createCluster(TestContext context, KubernetesClient mockClient) {
        ConfigMapOperations cmops = new ConfigMapOperations(vertx, mockClient);
        ServiceOperations svcops = new ServiceOperations(vertx, mockClient);
        KafkaSetOperations ksops = new KafkaSetOperations(vertx, mockClient);
        ZookeeperSetOperations zksops = new ZookeeperSetOperations(vertx, mockClient);
        DeploymentOperations depops = new DeploymentOperations(vertx, mockClient);
        PvcOperations pvcops = new PvcOperations(vertx, mockClient);
        KafkaClusterOperations kco = new KafkaClusterOperations(vertx, true, 2_000,
                cmops, svcops, zksops, ksops, pvcops, depops);

        LOGGER.info("Reconciling initially -> create");
        Async createAsync = context.async();
        kco.createOrUpdate(NAMESPACE, cluster.getMetadata().getName(), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            createAsync.complete();
        });
        createAsync.await();
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdateDelete(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        KubernetesClient mockClient = new MockKube().withInitialCms(Collections.singleton(cluster)
        ).build();

        KafkaClusterOperations kco = createCluster(context, mockClient);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.createOrUpdate(NAMESPACE, clusterName, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();
        LOGGER.info("Reconciling again -> delete");
        mockClient.configMaps().inNamespace(NAMESPACE).withName(clusterName).delete();
        Async deleteAsync = context.async();
        kco.delete(NAMESPACE, clusterName, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            deleteAsync.complete();
        });
    }

    /**
     * Test the controller re-creates services if they get deleted
     */
    private void updateClusterWithoutServices(TestContext context, String... services) {
        String clusterName = cluster.getMetadata().getName();
        KubernetesClient mockClient = new MockKube().withInitialCms(Collections.singleton(cluster)
        ).build();

        KafkaClusterOperations kco = createCluster(context, mockClient);
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertNull("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get());
        }
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.createOrUpdate(NAMESPACE, clusterName, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String service: services) {
                assertNotNull(
                        "Expected service " + service + " to have been recreated",
                        mockClient.services().inNamespace(NAMESPACE).withName(service).get());
            }
            updateAsync.complete();
        });
    }

    @Test
    public void testUpdateClusterWithoutZkServices(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        updateClusterWithoutServices(context,
                clusterName + "-zookeeper", clusterName + "-zookeeper-headless");
    }

    @Test
    public void testUpdateClusterWithoutKafkaServices(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        updateClusterWithoutServices(context,
                clusterName + "-kafka", clusterName + "-kafka-headless");
    }

    private void deleteClusterWithoutServices(TestContext context, String... services) {
        String clusterName = cluster.getMetadata().getName();
        KubernetesClient mockClient = new MockKube().withInitialCms(Collections.singleton(cluster)
        ).build();

        KafkaClusterOperations kco = createCluster(context, mockClient);
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertNull("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get());
        }
        LOGGER.info("Deleting");
        Async updateAsync = context.async();
        kco.delete(NAMESPACE, clusterName, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String service: services) {
                assertNull(
                        "Expected service " + service + " to still not exist",
                        mockClient.services().inNamespace(NAMESPACE).withName(service).get());
            }
            // TODO assert other resources do not exist either
            updateAsync.complete();
        });
    }

    @Test
    public void testDeleteClusterWithoutZkServices(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        deleteClusterWithoutServices(context,
                clusterName + "-zookeeper", clusterName + "-zookeeper-headless");
    }

    @Test
    public void testDeleteClusterWithoutKafkaServices(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        deleteClusterWithoutServices(context,
                clusterName + "-kafka", clusterName + "-kafka-headless");
    }

    private void deleteClusterWithoutStatefulSet(TestContext context, String... statefulSets) {
        String clusterName = cluster.getMetadata().getName();
        KubernetesClient mockClient = new MockKube().withInitialCms(Collections.singleton(cluster)
        ).build();

        KafkaClusterOperations kco = createCluster(context, mockClient);
        for (String ss: statefulSets) {
            mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).delete();
            assertNull("Expected ss " + ss + " to be not exist",
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).get());
        }
        LOGGER.info("Deleting");
        Async updateAsync = context.async();
        kco.delete(NAMESPACE, clusterName, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String ss: statefulSets) {
                assertNull(
                        "Expected ss " + ss + " to still not exist",
                        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).get());
            }
            // TODO assert other resources do not exist either
            updateAsync.complete();
        });
    }

    @Test
    public void testDeleteClusterWithoutZkStatefulSet(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        deleteClusterWithoutStatefulSet(context,
                clusterName + "-zookeeper");
    }

    @Test
    public void testDeleteClusterWithoutKafkaStatefulSet(TestContext context) {
        String clusterName = cluster.getMetadata().getName();
        deleteClusterWithoutStatefulSet(context,
                clusterName + "-kafka");
    }


}
