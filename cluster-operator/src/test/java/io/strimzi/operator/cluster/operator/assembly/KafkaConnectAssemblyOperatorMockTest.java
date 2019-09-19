/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

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
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectAssemblyOperatorMockTest.class);

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-connect-cluster";

    private final int replicas = 3;

    private KubernetesClient mockClient;

    private Vertx vertx;
    private MockKube mockKube;

    @BeforeEach
    public void before() {
        this.vertx = Vertx.vertx();
    }

    private void setConnectResource(KafkaConnect connectResource) {
        if (mockClient != null) {
            mockClient.close();
        }
        mockKube = new MockKube();
        mockClient = mockKube
                .withCustomResourceDefinition(Crds.kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class, KafkaConnect::getStatus, KafkaConnect::setStatus)
                    .withInitialInstances(Collections.singleton(connectResource))
                .end()
                .withCustomResourceDefinition(Crds.kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class, KafkaConnector::getStatus, KafkaConnector::setStatus)
                .end()
                .build();
    }

    @AfterEach
    public void after() {
        if (mockClient != null) {
            mockClient.close();
        }
        this.vertx.close();
    }


    private KafkaConnectAssemblyOperator createConnectCluster(VertxTestContext context, KafkaConnectApi kafkaConnectApi)  throws InterruptedException, ExecutionException, TimeoutException {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_9);
        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(this.vertx, this.mockClient, pfa, 60_000L);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        KafkaConnectAssemblyOperator kco = new KafkaConnectAssemblyOperator(vertx, pfa,
            supplier,
            config,
            foo -> {
                return kafkaConnectApi;
            });

        LOGGER.info("Reconciling initially -> create");
        CountDownLatch createAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            context.verify(() -> assertThat(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(KafkaConnectResources.deploymentName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaConnectResources.metricsAndLogConfigMapName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.services().inNamespace(NAMESPACE).withName(KafkaConnectResources.serviceName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.policy().podDisruptionBudget().inNamespace(NAMESPACE).withName(KafkaConnectResources.deploymentName(CLUSTER_NAME)).get(), is(notNullValue())));
            createAsync.countDown();
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdate(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
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
        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        KafkaConnectAssemblyOperator kco = createConnectCluster(context,
                mock);
        LOGGER.info("Reconciling again -> update");
        CountDownLatch updateAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertTrue(ar.succeeded()));
            updateAsync.countDown();
        });
        updateAsync.await(30, TimeUnit.SECONDS);
    }

    /*
     * TODO Implement the KafkaConnect vs S2I preference
     * TODO implement toggle in KafkaConnect.spec. (how do we change the default?)
     * Create connect, create connector, delete connector, delete connect
     * Create connect, create connector, delete connect, delete connector
     * Create connector, create connect, delete connector, delete connect
     * Create connector, create connect, delete connect, delete connector
     * Create connect1, create connect2, create connector for 1, move to connector for 2, delete connector, delete connect
     */

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    @Ignore
    public void testCreateUpdateWithSelector(VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        // KafkaConnector Config
        String connectorAName = "my-connector-a";
        String connectorBName = "my-connector-b";

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
                .withReplicas(replicas)
                .endSpec()
                .build();
        setConnectResource(connectCluster);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.createOrUpdatePutRequest(anyString(), anyInt(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockConnectApi.delete(anyString(), anyInt(), any())).thenReturn(Future.succeededFuture());
        when(mockConnectApi.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        KafkaConnectAssemblyOperator kco = createConnectCluster(context, mockConnectApi);
        LOGGER.info("Reconciling again -> update");
        Checkpoint updateCheckpoint = context.checkpoint();
        CountDownLatch updateAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            updateCheckpoint.flag();
            updateAsync.countDown();
        });

        updateAsync.await(30, TimeUnit.SECONDS);

        // Create connector A which matches the selector
        KafkaConnector connectorA = Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorAName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)
                .endMetadata()
                .withNewSpec()
                    .withClassName("my.connector.ClassA")
                    .withTasksMax(4)
                    .withConfig(kakfaConnectorConfig)
                .endSpec()
            .done();

        // Create a connector B which does not match the selector
        KafkaConnector connectorB = Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorBName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME + "2")
                .endMetadata()
                .withNewSpec()
                    .withClassName("my.connector.ClassB")
                    .withTasksMax(4)
                    .withConfig(any())
                .endSpec()
            .done();

        Thread.sleep(3000); // TODO fix this

        // Verify the REST API call, and that we've got a single watch
        //mockKube.assertNumWatchers(KafkaConnect.class, 1);
        //context.assertEquals(1, mockKube.watchers(KafkaConnector.class).size());
        verify(mockConnectApi).createOrUpdatePutRequest(anyString(), anyInt(), eq(connectorAName), any(JsonObject.class));

        // Change the connect cluster
        when(mockConnectApi.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(connectorAName)));
        Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).withName(connectorAName).patch(
            new KafkaConnectorBuilder(connectorA)
                .editMetadata()
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME + "2")
                .endMetadata()
            .build());
        Checkpoint updateCheckpoint2 = context.checkpoint();
        CountDownLatch updateAsync2 = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertTrue(ar.succeeded()));
            updateCheckpoint2.flag();
            updateAsync2.countDown();
        });
        updateAsync2.await(30, TimeUnit.SECONDS);

        Thread.sleep(3000); // TODO fix this

        // Verify we delete the connector which no longer matches
        verify(mockConnectApi).delete(anyString(), anyInt(), eq(connectorAName));
        // Verify we created the connector which does now match
        verify(mockConnectApi).createOrUpdatePutRequest(anyString(), anyInt(), eq(connectorBName), any(JsonObject.class));


        // Delete the connector
        when(mockConnectApi.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(connectorBName)));
        Crds.kafkaConnectorOperation(mockClient).inNamespace(NAMESPACE).withName(connectorBName).delete();

        Thread.sleep(3000);

        // Verify the connector was deleted
        verify(mockConnectApi).delete(anyString(), anyInt(), eq(connectorBName));

        Crds.kafkaConnectOperation(mockClient).inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Checkpoint updateCheckpoint3 = context.checkpoint();
        CountDownLatch updateAsync3 = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertTrue(ar.succeeded()));
            updateCheckpoint3.flag();
            updateAsync3.countDown();
        });
        updateAsync3.await(30, TimeUnit.SECONDS);
        Thread.sleep(3000);

        //context.async();
    }

}
