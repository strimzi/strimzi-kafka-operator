/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.ConnectorPluginBuilder;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.set;
import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ConnectorMockTest {

    private static final String NAMESPACE = "ns";

    static class ConnectorState {
        boolean paused;
        JsonObject config;

        public ConnectorState(boolean paused, JsonObject config) {
            this.paused = paused;
            this.config = config;
        }
    }

    private Vertx vertx;
    private KubernetesClient client;
    private KafkaConnectApi api;
    private HashMap<String, ConnectorState> runningConnectors;
    private KafkaConnectS2IAssemblyOperator kafkaConnectS2iOperator;
    private KafkaConnectAssemblyOperator kafkaConnectOperator;

    String key(String host, String connectorName) {
        return host + "##" + connectorName;
    }

    private Future<Map<String, Object>> kafkaConnectApiStatusMock(String host, int port, String connectorName)   {
        ConnectorState connectorState = runningConnectors.get(key(host, connectorName));
        Map<String, Object> statusNode = new HashMap<>();
        statusNode.put("name", connectorName);
        Map<String, Object> connector = new HashMap<>();
        statusNode.put("connector", connector);
        connector.put("state", connectorState.paused ? "PAUSED" : "RUNNING");
        connector.put("worker_id", "somehost0:8083");
        Map<String, Object> task = new HashMap<>();
        task.put("id", 0);
        task.put("state", connectorState.paused ? "PAUSED" : "RUNNING");
        task.put("worker_id", "somehost2:8083");
        List<Map> tasks = singletonList(task);
        statusNode.put("tasks", tasks);

        return connectorState != null ? Future.succeededFuture(statusNode) : Future.failedFuture("No such connector " + connectorName);
    }

    @BeforeEach
    public void setup(VertxTestContext testContext) throws InterruptedException {
        vertx = Vertx.vertx();
        client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class,
                        KafkaConnect::getStatus, KafkaConnect::setStatus).end()
                .withCustomResourceDefinition(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class,
                        KafkaConnectS2I::getStatus, KafkaConnectS2I::setStatus).end()
                .withCustomResourceDefinition(Crds.kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class,
                        KafkaConnector::getStatus, KafkaConnector::setStatus).end()
                .build();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_11);
        MockCertManager certManager = new MockCertManager();

        api = mock(KafkaConnectApi.class);
        runningConnectors = new HashMap<>();
        when(api.list(any(), anyInt())).thenAnswer(i -> {
            String host = i.getArgument(0);
            return Future.succeededFuture(runningConnectors.keySet().stream()
                    .filter(s -> s.startsWith(host + "##"))
                    .map(s -> s.substring(host.length() + 2))
                    .collect(Collectors.toList()));
        });
        when(api.listConnectorPlugins(any(), anyInt())).thenAnswer(i -> {
            ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                    .withConnectorClass("io.strimzi.MyClass")
                    .withType("sink")
                    .withVersion("1.0.0")
                    .build();
            return Future.succeededFuture(Collections.singletonList(plugin1));
        });
        when(api.getConnectorConfig(any(), any(), anyInt(), any())).thenAnswer(invocation -> {
            String host = invocation.getArgument(1);
            String connectorName = invocation.getArgument(3);
            ConnectorState connectorState = runningConnectors.get(key(host, connectorName));
            if (connectorState != null) {
                Map<String, Object> map = new HashMap<>();
                for (Map.Entry<String, Object> entry : connectorState.config) {
                    map.put(entry.getKey(), entry.getValue());
                }
                return Future.succeededFuture(map);
            } else {
                return Future.failedFuture(new ConnectRestException("GET", String.format("/connectors/%s/config", connectorName), 404, "Not Found", ""));
            }
        });
        when(api.getConnector(any(), anyInt(), any())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            String connectorName = invocation.getArgument(3);
            ConnectorState connectorState = runningConnectors.get(key(host, connectorName));
            if (connectorState != null) {
                return Future.succeededFuture(TestUtils.map(
                        "name", connectorName,
                        "config", connectorState.config,
                        "tasks", emptyMap()));
            } else {
                return Future.failedFuture(new ConnectRestException("GET", String.format("/connectors/%s", connectorName), 404, "Not Found", ""));
            }
        });
        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            System.err.println("###### create " + host);
            String connectorName = invocation.getArgument(2);
            JsonObject connectorConfig = invocation.getArgument(3);
            runningConnectors.putIfAbsent(key(host, connectorName), new ConnectorState(false, connectorConfig));
            return Future.succeededFuture();
        });
        when(api.delete(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            System.err.println("###### delete " + host);
            String connectorName = invocation.getArgument(2);
            ConnectorState remove = runningConnectors.remove(key(host, connectorName));
            return remove != null ? Future.succeededFuture() : Future.failedFuture("No such connector " + connectorName);
        });
        when(api.statusWithBackOff(any(), any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(1);
            System.err.println("###### status " + host);
            return kafkaConnectApiStatusMock(invocation.getArgument(1), invocation.getArgument(2), invocation.getArgument(3));
        });
        when(api.status(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            System.err.println("###### status " + host);
            return kafkaConnectApiStatusMock(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
        });
        when(api.pause(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            String connectorName = invocation.getArgument(2);
            ConnectorState connectorState = runningConnectors.get(key(host, connectorName));
            if (connectorState == null) {
                return Future.failedFuture(new ConnectRestException("PUT", "", 404, "Not found", "Connector name " + connectorName));
            }
            if (!connectorState.paused) {
                runningConnectors.put(key(host, connectorName), new ConnectorState(true, connectorState.config));
            }
            return Future.succeededFuture();
        });
        when(api.resume(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            String connectorName = invocation.getArgument(2);
            ConnectorState connectorState = runningConnectors.get(key(host, connectorName));
            if (connectorState == null) {
                return Future.failedFuture(new ConnectRestException("PUT", "", 404, "Not found", "Connector name " + connectorName));
            }
            if (connectorState.paused) {
                runningConnectors.put(key(host, connectorName), new ConnectorState(false, connectorState.config));
            }
            return Future.succeededFuture();
        });


        ResourceOperatorSupplier ros = new ResourceOperatorSupplier(vertx, client, pfa, 10_000);
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(map(
            ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString(),
            ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString(),
            ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, KafkaVersionTestUtils.getKafkaConnectS2iImagesEnvVarString(),
            ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString(),
            ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, Long.toString(Long.MAX_VALUE)),
                KafkaVersionTestUtils.getKafkaVersionLookup());
        kafkaConnectOperator = new KafkaConnectAssemblyOperator(vertx,
            pfa,
            ros,
            config,
            x -> api);
        CountDownLatch async2 = new CountDownLatch(1);
        kafkaConnectOperator.createWatch(NAMESPACE, e -> testContext.failNow(e)).setHandler(asyncResultHandler(testContext, async2));
        async2.await(30, TimeUnit.SECONDS);
        kafkaConnectS2iOperator = new KafkaConnectS2IAssemblyOperator(vertx,
            pfa,
            ros,
            config,
            x -> api);
        CountDownLatch async1 = new CountDownLatch(1);
        kafkaConnectS2iOperator.createWatch(NAMESPACE, e -> testContext.failNow(e)).setHandler(asyncResultHandler(testContext, async1));
        async1.await(30, TimeUnit.SECONDS);
        CountDownLatch async = new CountDownLatch(1);
        AbstractConnectOperator.createConnectorWatch(kafkaConnectOperator, kafkaConnectS2iOperator, NAMESPACE).setHandler(asyncResultHandler(testContext, async));
        async.await(30, TimeUnit.SECONDS);

        testContext.completeNow();
    }

    @AfterEach
    public void teardown() {
        vertx.close();
    }

    public <T> Handler<AsyncResult<T>> asyncResultHandler(VertxTestContext testContext, CountDownLatch async) {
        return ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
            }
            async.countDown();
        };
    }

    private static <T extends HasMetadata & HasStatus<?>> Predicate<T> statusIsForCurrentGeneration() {
        return c -> c.getStatus() != null
                && c.getMetadata().getGeneration() != null
                && c.getMetadata().getGeneration().equals(c.getStatus().getObservedGeneration());
    }

    private static <T extends HasStatus<?>> Predicate<T> notReady(String reason, String message) {
        return c -> c.getStatus() != null
                && c.getStatus().getConditions().stream()
                .anyMatch(condition ->
                        "NotReady".equals(condition.getType())
                                && "True".equals(condition.getStatus())
                                && reason.equals(condition.getReason())
                                && Objects.equals(message, condition.getMessage())
                );
    }

    private static <T extends HasStatus<?>> Predicate<T> ready() {
        return c -> c.getStatus() != null
                && c.getStatus().getConditions().stream()
                .anyMatch(condition ->
                        "Ready".equals(condition.getType())
                                && "True".equals(condition.getStatus())
                );
    }

    public <T extends HasMetadata & HasStatus<?>> void waitForStatus(Resource<T, ?> resource, String resourceName, Predicate<T> predicate) {
        try {
            resource.waitUntilCondition(predicate, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            fail(resourceName + " never matched required predicate: " + conditions);
        }
    }

    public void waitForConnectReady(String connectName) {
        Resource<KafkaConnect, DoneableKafkaConnect> resource = Crds.kafkaConnectOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        waitForStatus(resource, connectName, ready());
    }

    public void waitForConnectNotReady(String connectName, String reason, String message) {
        Resource<KafkaConnect, DoneableKafkaConnect> resource = Crds.kafkaConnectOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        waitForStatus(resource, connectName,
                ConnectorMockTest.<KafkaConnect>statusIsForCurrentGeneration().and(notReady(reason, message)));
    }

    public void waitForConnectS2iReady(VertxTestContext testContext, String connectName) {
        Resource<KafkaConnectS2I, DoneableKafkaConnectS2I> resource = Crds.kafkaConnectS2iOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        waitForStatus(resource, connectName, ready());
    }

    public void waitForConnectS2iNotReady(String connectName, String reason, String message) {
        Resource<KafkaConnectS2I, DoneableKafkaConnectS2I> resource = Crds.kafkaConnectS2iOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        waitForStatus(resource, connectName,
                ConnectorMockTest.<KafkaConnectS2I>statusIsForCurrentGeneration().and(notReady(reason, message)));
    }

    public void waitForConnectorReady(String connectorName) {
        Resource<KafkaConnector, DoneableKafkaConnector> resource = Crds.kafkaConnectorOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectorName);
        waitForStatus(resource, connectorName, ready());
    }

    public void waitForConnectorState(String connectorName, String state) {
        Resource<KafkaConnector, DoneableKafkaConnector> resource = Crds.kafkaConnectorOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectorName);
        waitForStatus(resource, connectorName, s -> {
            Map<String, Object> connector = s.getStatus().getConnectorStatus();
            if (connector != null) {
                Object connectorState = ((Map) connector.getOrDefault("connector", emptyMap())).get("state");
                return connectorState instanceof String
                    && state.equals(connectorState);
            } else {
                return false;
            }
        });
    }

    public void waitForConnectorNotReady(String connectorName, String reason, String message) {
        Resource<KafkaConnector, DoneableKafkaConnector> resource = Crds.kafkaConnectorOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectorName);
        waitForStatus(resource, connectorName,
                ConnectorMockTest.<KafkaConnector>statusIsForCurrentGeneration().and(notReady(reason, message)));
    }

    @Test
    public void testConnectWithoutSpec() {
        String connectName = "cluster";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                .endMetadata()
                .done();
        waitForConnectNotReady(connectName, "InvalidResourceException", "spec property is required");
        return;
    }

    @Test
    public void testConnectorWithoutLabel() {
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectorName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorNotReady(connectorName, "InvalidResourceException",
                "Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No connect cluster in which to create this connector.");
        return;
    }

    @Test
    public void testConnectorButConnectDoesNotExist() {
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(connectorName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, "cluster")
                .endMetadata()
                .done();
        waitForConnectorNotReady(connectorName, "NoSuchResourceException",
                "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");
        return;
    }

    @Test
    public void testConnectorWithoutSpec() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .done();
        waitForConnectorNotReady(connectorName, "InvalidResourceException", "spec property is required");
        return;
    }

    @Test
    public void testConnectorButConnectNotConfiguredForConnectors() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    //.addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec().endSpec()
                .done();
        assertNotNull(Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).get());
        waitForConnectorNotReady(connectorName, "NoSuchResourceException",
                "KafkaConnect cluster is not configured with annotation " + Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES);
    }

    /** Create connect, create connector, delete connector, delete connect */
    @Test
    public void testConnectConnectorConnectorConnect() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
            .done();
        waitForConnectReady(connectName);

        // triggered twice (creation+status update)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec()
            .done();
        waitForConnectorReady(connectorName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(key("cluster-connect-api.ns.svc", connectorName)));
        
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        //waitForConnectReady(testContext, connectName);
        waitFor("delete call on connect REST api", 1_000, 30_000, () -> {
            return runningConnectors.isEmpty();
        });
        verify(api).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
    }

    /** Create connector, create connect, delete connector, delete connect */
    @Test
    public void testConnectorConnectConnectorConnect() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorNotReady(connectorName, "NoSuchResourceException",
            "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");

        verify(api, never()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), emptySet());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);
        // (connect crt, connector status, connect status)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(key("cluster-connect-api.ns.svc", connectorName)));

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        //waitForConnectReady(testContext, connectName);
        waitFor("delete call on connect REST api", 1_000, 30_000, () -> {
            return runningConnectors.isEmpty();
        });
        verify(api).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
    }

    /** Create connect, create connector, delete connect, delete connector */
    @Test
    public void testConnectConnectorConnectConnector() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec()
                .done();
        waitForConnectorReady(connectorName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(key("cluster-connect-api.ns.svc", connectorName)));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        waitForConnectorNotReady(connectorName,
                "NoSuchResourceException", "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        verify(api, never()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Create connector, create connect, delete connect, delete connector */
    @Test
    public void testConnectorConnectConnectConnector() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorNotReady(connectorName, "NoSuchResourceException",
                "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");

        verify(api, never()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), emptySet());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(key("cluster-connect-api.ns.svc", connectorName)));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        waitForConnectorNotReady(connectorName,
                "NoSuchResourceException", "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        verify(api, never()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Change the cluster label from one cluster to another; check the connector is deleted from the old cluster */
    @Test
    public void testChangeLabel() throws InterruptedException {
        String connect1Name = "cluster1";
        String connect2Name = "cluster2";
        String connectorName = "connector";

        // Create two connect clusters
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connect1Name)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connect1Name);
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connect2Name)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connect2Name);

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connect1Name)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec()
                .done();
        waitForConnectorReady(connectorName);

        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connect1Name, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connect2Name, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).patch(new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connect2Name)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec().build());
        waitForConnectorReady(connectorName);

        // Note: The connector does not get deleted immediately from cluster 1, only on the next timed reconciliation

        verify(api, never()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connect1Name, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connect2Name, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        CountDownLatch async = new CountDownLatch(1);
        kafkaConnectOperator.reconcile(new Reconciliation("test", "KafkaConnect", NAMESPACE, connect1Name)).setHandler(ar -> {
            async.countDown();
        });
        async.await(30, TimeUnit.SECONDS);
        verify(api, atLeastOnce()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connect1Name, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Create connect, create connector, delete connector, delete connect */
    @Test
    public void testExceptionFromRestApi() {
        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any())).thenAnswer(invocation -> {
            return Future.failedFuture(new ConnectRestException("GET", "/foo", 500, "Internal server error", "Bad stuff happened"));
        });
        runningConnectors.clear();
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(connectName)
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (creation+status update)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorNotReady(connectorName,
                "ConnectRestException", "GET /foo returned 500 (Internal server error): Bad stuff happened");

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), emptySet());
    }

    /** Create connect, create connector, delete connector, delete connect */
    @Test
    public void testPauseResume() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(connectName)
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (creation+status update)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec()
                .done();
        waitForConnectorReady(connectorName);
        waitForConnectorState(connectorName, "RUNNING");

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), singleton(key("cluster-connect-api.ns.svc", connectorName)));

        verify(api, never()).pause(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, never()).resume(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).edit()
            .editSpec()
                .withPause(true)
            .endSpec()
        .done();

        waitForConnectorState(connectorName, "PAUSED");

        verify(api, atLeastOnce()).pause(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, never()).resume(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).edit()
                .editSpec()
                .withPause(false)
                .endSpec()
                .done();

        waitForConnectorState(connectorName, "RUNNING");

        verify(api, atLeastOnce()).pause(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, atLeastOnce()).resume(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

}
