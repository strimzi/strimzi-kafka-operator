/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.ServiceOperator;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static io.strimzi.operator.cluster.ResourceUtils.map;

@RunWith(VertxUnitRunner.class)
public class KafkaConnectAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-connect-cluster";

    private final int replicas = 3;

    private KubernetesClient mockClient;

    private Vertx vertx;
    private ConfigMap cluster;

    @Before
    public void before() {
        this.vertx = Vertx.vertx();

        this.cluster = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Labels.forKind("cluster").withType(AssemblyType.CONNECT).toMap())
                .endMetadata()
                .withData(map(KafkaConnectCluster.KEY_REPLICAS, String.valueOf(replicas),
                        KafkaConnectCluster.KEY_CONNECT_CONFIG, "{}",
                        KafkaConnectCluster.KEY_METRICS_CONFIG, "{}",
                        KafkaConnectCluster.KEY_RESOURCES, null))
                .build();
        mockClient = new MockKube().withInitialCms(Collections.singleton(cluster)).build();
    }

    @After
    public void after() {
        this.vertx.close();
    }

    private KafkaConnectAssemblyOperator createConnectCluster(TestContext context) {
        ConfigMapOperator cmops = new ConfigMapOperator(vertx, mockClient);
        ServiceOperator svcops = new ServiceOperator(vertx, mockClient);
        DeploymentOperator depops = new DeploymentOperator(vertx, mockClient);
        KafkaConnectAssemblyOperator kco = new KafkaConnectAssemblyOperator(vertx, true,
                cmops, depops, svcops);

        LOGGER.info("Reconciling initially -> create");
        Async createAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            context.assertNotNull(mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(KafkaConnectCluster.kafkaConnectClusterName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaConnectCluster.metricsConfigName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.services().inNamespace(NAMESPACE).withName(KafkaConnectCluster.kafkaConnectClusterName(CLUSTER_NAME)).get());
            createAsync.complete();
        });
        createAsync.await();
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdateDelete(TestContext context) {
        KafkaConnectAssemblyOperator kco = createConnectCluster(context);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();
        LOGGER.info("Reconciling again -> delete");
        mockClient.configMaps().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Async deleteAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            context.assertNull(mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(KafkaConnectCluster.kafkaConnectClusterName(CLUSTER_NAME)).get());
            //FIXME: context.assertNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaConnectCluster.metricsConfigName(CLUSTER_NAME)).get());
            context.assertNull(mockClient.services().inNamespace(NAMESPACE).withName(KafkaConnectCluster.kafkaConnectClusterName(CLUSTER_NAME)).get());
            deleteAsync.complete();
        });
    }
}
