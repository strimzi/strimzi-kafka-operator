/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaConnectAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectAssemblyOperatorMockTest.class);

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef 2.0.x"),
            emptyMap(), singletonMap("2.0.0", "strimzi/kafka-connect:latest-kafka-2.0.0"), emptyMap(), emptyMap()) { };

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-connect-cluster";

    private final int replicas = 3;

    private KubernetesClient mockClient;

    private Vertx vertx;
    private MockKube mockKube;

    @Before
    public void before() {
        this.vertx = Vertx.vertx();
    }

    private void setConnectResource(KafkaConnect connectResource) {
        mockKube = new MockKube();
        mockClient = mockKube
                .withCustomResourceDefinition(Crds.kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class)
                    .withInitialInstances(Collections.singleton(connectResource))
                .end()
                .withCustomResourceDefinition(Crds.kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class)
                .end()
                .build();
    }

    @After
    public void after() {
        this.vertx.close();
    }

    private KafkaConnectAssemblyOperator createConnectCluster(TestContext context, KafkaConnectApi kafkaConnectApi) {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_9);
        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(this.vertx, this.mockClient, pfa, 60_000L);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        KafkaConnectAssemblyOperator kco = new KafkaConnectAssemblyOperator(vertx, pfa,
            new MockCertManager(),
            supplier,
            config,
            foo -> {
                return kafkaConnectApi;
            });

        LOGGER.info("Reconciling initially -> create");
        Async createAsync = context.async();
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            context.assertNotNull(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(KafkaConnectResources.deploymentName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaConnectResources.metricsAndLogConfigMapName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.services().inNamespace(NAMESPACE).withName(KafkaConnectResources.serviceName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.policy().podDisruptionBudget().inNamespace(NAMESPACE).withName(KafkaConnectResources.deploymentName(CLUSTER_NAME)).get());
            createAsync.complete();
        });
        createAsync.await();
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdate(TestContext context) {
        setConnectResource(new KafkaConnectBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(CLUSTER_NAME)
                        .withNamespace(NAMESPACE)
                        .withLabels(Labels.userLabels(TestUtils.map("foo", "bar")).toMap())
                        .build())
                .withNewSpec()
                .withReplicas(replicas)
                .endSpec()
            .build());
        KafkaConnectApi mock = mock(KafkaConnectApi.class, invocation -> {
            throw new RuntimeException();
        });
        KafkaConnectAssemblyOperator kco = createConnectCluster(context,
                mock);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdateWithSelector(TestContext context) throws InterruptedException {
        // KafkaConnector Config
        Map<String, Object> kakfaConnectorConfig = new HashMap<>();
        kakfaConnectorConfig.put("my.config", "true");

        // Create a connect with a selector
        KafkaConnect connectCluster = new KafkaConnectBuilder()
                .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Labels.userLabels(TestUtils.map("foo", "bar")).toMap())
                .endMetadata()
                .withNewSpec()
                .withConnectorSelector(new LabelSelectorBuilder().addToMatchLabels("connect", "A").build())
                .withReplicas(replicas)
                .endSpec()
                .build();
        setConnectResource(connectCluster);
        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.createOrUpdatePutRequest(anyString(), any())).thenReturn(Future.succeededFuture());
        when(mock.delete(any())).thenReturn(Future.succeededFuture());
        KafkaConnectAssemblyOperator kco = createConnectCluster(context, mock);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();

        // Create connector A which matches the selector
        Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).create(
            new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector-a")
                    .withNamespace(NAMESPACE)
                    .addToLabels("connect", "A")
                .endMetadata()
                .withNewSpec()
                    .withClassName("my.connector.ClassA")
                    .withTasksMax(4)
                    .withConfig(kakfaConnectorConfig)
                .endSpec()
            .build());

        // Create a connector B which does not match the selector
        Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).create(
            new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector-b")
                    .withNamespace(NAMESPACE)
                    .addToLabels("connect", "B")
                .endMetadata()
                .withNewSpec()
                    .withClassName("my.connector.ClassB")
                    .withTasksMax(4)
                    .withConfig(any())
                .endSpec()
            .build());

        Thread.sleep(3000);

        // Verify the REST API call, and that we've got a single watch
        context.assertEquals(0, mockKube.watchers(KafkaConnect.class).size());
        context.assertEquals(1, mockKube.watchers(KafkaConnector.class).size());
        verify(mock).createOrUpdatePutRequest(eq("my-connector-a"), any(JsonObject.class));

        // Change the selector used by connect
        when(mock.list()).thenReturn(Future.succeededFuture(singletonList("my-connector-a")));
        Crds.kafkaConnectOperation(mockClient).inNamespace(NAMESPACE).withName(CLUSTER_NAME).patch(
            new KafkaConnectBuilder(connectCluster)
                .editSpec()
                    .withConnectorSelector(new LabelSelectorBuilder().addToMatchLabels("connect", "B").build())
                .endSpec()
            .build());
        Async updateAsync2 = context.async();
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync2.complete();
        });
        updateAsync2.await();

        Thread.sleep(3000);

        // Verify we delete the connector which no longer matches
        verify(mock).delete(eq("my-connector-a"));
        // Verify we created the connector which does now match
        verify(mock).createOrUpdatePutRequest(eq("my-connector-b"), any(JsonObject.class));
        // Verify we still only have one watch
        context.assertEquals(0, mockKube.watchers(KafkaConnect.class).size());
        context.assertEquals(1, mockKube.watchers(KafkaConnector.class).size());

        // Delete the connector
        when(mock.list()).thenReturn(Future.succeededFuture(singletonList("my-connector-b")));
        Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).withName("my-connector-b").delete();

        Thread.sleep(3000);

        // Verify the connector was deleted
        verify(mock).delete(eq("my-connector-b"));
        context.assertEquals(0, mockKube.watchers(KafkaConnect.class).size());
        context.assertEquals(1, mockKube.watchers(KafkaConnector.class).size());

        Crds.kafkaConnectOperation(mockClient).inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Async updateAsync3 = context.async();
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync3.complete();
        });
        updateAsync3.await();
        Thread.sleep(3000);
        context.assertEquals(0, mockKube.watchers(KafkaConnect.class).size());
        context.assertEquals(0, mockKube.watchers(KafkaConnector.class).size());

        //context.async();
    }

}
