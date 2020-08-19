/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.netty.channel.ConnectTimeoutException;
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
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.operator.resource.DefaultZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ConnectorMockTest {

    private static final Logger log = LogManager.getLogger(ConnectorMockTest.class.getName());

    private static final String NAMESPACE = "ns";

    static class ConnectorState {
        boolean paused;
        JsonObject config;

        public ConnectorState(boolean paused, JsonObject config) {
            this.paused = paused;
            this.config = config;
        }
    }

    private static Vertx vertx;
    private KubernetesClient client;
    private KafkaConnectApi api;
    private HashMap<String, ConnectorState> runningConnectors;
    private KafkaConnectS2IAssemblyOperator kafkaConnectS2iOperator;
    private KafkaConnectAssemblyOperator kafkaConnectOperator;

    String key(String host, String connectorName) {
        return host + "##" + connectorName;
    }

    private Future<Map<String, Object>> kafkaConnectApiStatusMock(String host, String connectorName)   {
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

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @BeforeEach
    public void setup(VertxTestContext testContext) {
        client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class,
                        KafkaConnect::getStatus, KafkaConnect::setStatus).end()
                .withCustomResourceDefinition(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class,
                        KafkaConnectS2I::getStatus, KafkaConnectS2I::setStatus).end()
                .withCustomResourceDefinition(Crds.kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class,
                        KafkaConnector::getStatus, KafkaConnector::setStatus).end()
                .build();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_11);

        api = mock(KafkaConnectApi.class);
        runningConnectors = new HashMap<>();

        when(api.list(any(), anyInt())).thenAnswer(i -> {
            String host = i.getArgument(0);
            String matchingKeyPrefix = host + "##";
            return Future.succeededFuture(runningConnectors.keySet().stream()
                    .filter(s -> s.startsWith(matchingKeyPrefix))
                    .map(s -> s.substring(matchingKeyPrefix.length()))
                    .collect(Collectors.toList()));
        });
        when(api.listConnectorPlugins(any(), anyInt())).thenAnswer(i -> {
            ConnectorPlugin connectorPlugin = new ConnectorPluginBuilder()
                    .withConnectorClass("io.strimzi.MyClass")
                    .withType("sink")
                    .withVersion("1.0.0")
                    .build();
            return Future.succeededFuture(Collections.singletonList(connectorPlugin));
        });
        when(api.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());
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
            String connectorName = invocation.getArgument(2);
            ConnectorState connectorState = runningConnectors.get(key(host, connectorName));
            if (connectorState == null) {
                return Future.failedFuture(new ConnectRestException("GET", String.format("/connectors/%s", connectorName), 404, "Not Found", ""));
            }
            return Future.succeededFuture(TestUtils.map(
                    "name", connectorName,
                    "config", connectorState.config,
                    "tasks", emptyMap()));
        });
        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any())).thenAnswer(invocation -> {
            log.info((String) invocation.getArgument(0) + invocation.getArgument(1) + invocation.getArgument(2) + invocation.getArgument(3));
            String host = invocation.getArgument(0);
            log.info("###### create " + host);
            String connectorName = invocation.getArgument(2);
            JsonObject connectorConfig = invocation.getArgument(3);
            runningConnectors.putIfAbsent(key(host, connectorName), new ConnectorState(false, connectorConfig));
            return Future.succeededFuture();
        });
        when(api.delete(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            log.info("###### delete " + host);
            String connectorName = invocation.getArgument(2);
            ConnectorState remove = runningConnectors.remove(key(host, connectorName));
            return remove != null ? Future.succeededFuture() : Future.failedFuture("No such connector " + connectorName);
        });
        when(api.statusWithBackOff(any(), any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(1);
            log.info("###### status " + host);
            String connectorName = invocation.getArgument(3);
            return kafkaConnectApiStatusMock(host, connectorName);
        });
        when(api.status(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String host = invocation.getArgument(0);
            log.info("###### status " + host);
            String connectorName = invocation.getArgument(2);
            return kafkaConnectApiStatusMock(host, connectorName);
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

        ResourceOperatorSupplier ros = new ResourceOperatorSupplier(vertx, client,
                new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, client),
                    // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                    () -> new BackOff(5_000, 2, 4)),
                new DefaultAdminClientProvider(),
                new DefaultZookeeperScalerProvider(),
                ResourceUtils.metricsProvider(),
                pfa, 10_000);
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

        Checkpoint async = testContext.checkpoint();
        // Fail test if watcher closes for any reason
        kafkaConnectOperator.createWatch(NAMESPACE, e -> testContext.failNow(e))
            .onComplete(testContext.succeeding())
            .compose(watch -> {
                kafkaConnectS2iOperator = new KafkaConnectS2IAssemblyOperator(vertx,
                    pfa,
                    ros,
                    config,
                    x -> api);
                // Fail test if watcher closes for any reason
                return kafkaConnectS2iOperator.createWatch(NAMESPACE, e -> testContext.failNow(e));
            })
            .onComplete(testContext.succeeding())
            .compose(watch -> AbstractConnectOperator.createConnectorWatch(kafkaConnectOperator, kafkaConnectS2iOperator, NAMESPACE))
            .onComplete(testContext.succeeding(v -> async.flag()));
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
    public void testConnectNotReadyWithoutSpec() {
        String connectName = "cluster";

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                .endMetadata()
                .done();
        waitForConnectNotReady(connectName, "InvalidResourceException", "spec property is required");
    }

    @Test
    public void testConnectorNotReadyWithoutStrimziClusterLabel() {
        String connectorName = "connector";

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
    }

    @Test
    public void testConnectorNotReadyWhenConnectDoesNotExist() {
        String connectorName = "connector";

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, "cluster")
                .endMetadata()
                .done();
        waitForConnectorNotReady(connectorName, "NoSuchResourceException",
                "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");
    }

    @Test
    public void testConnectorNotReadyWithoutSpec() {
        String connectName = "cluster";
        String connectorName = "connector";

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .done();
        waitForConnectorNotReady(connectorName, "InvalidResourceException", "spec property is required");
    }

    @Test
    public void testConnectorNotReadyWhenConnectNotConfiguredForConnectors() {
        String connectName = "cluster";
        String connectorName = "connector";

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    //.addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec().endSpec()
                .done();
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
                    .withReplicas(1)
                .endSpec()
            .done();
        waitForConnectReady(connectName);

        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));

        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnector and wait till it's ready
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

        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));
        
        boolean connectorDeleted = Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        assertThat(connectorDeleted, is(true));
        waitFor("delete call on connect REST api", 1_000, 30_000,
            () -> runningConnectors.isEmpty());
        // Verify connector is deleted from the connect via REST api
        verify(api).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        boolean connectDeleted = Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        assertThat(connectDeleted, is(true));
    }

    /** Create connector, create connect, delete connector, delete connect */
    @Test
    public void testConnectorConnectConnectorConnect() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnector and wait till it's ready
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
        assertThat(runningConnectors.keySet(), is(empty()));

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);
        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        // triggered three times (Connect creation, Connector Status update, Connect Status update)
        verify(api, times(3)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));

        boolean connectorDeleted = Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        assertThat(connectorDeleted, is(true));
        waitFor("delete call on connect REST api", 1_000, 30_000,
            () -> runningConnectors.isEmpty());
        verify(api).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        boolean connectDeleted = Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        assertThat(connectDeleted, is(true));
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
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnector and wait till it's ready
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

        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        // triggered twice (Connect creation, Connector Status update)
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));

        boolean connectDeleted = Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        assertThat(connectDeleted, is(true));

        waitForConnectorNotReady(connectorName,
                "NoSuchResourceException", "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");

        boolean connectorDeleted = Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        assertThat(connectorDeleted, is(true));
        verify(api, never()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Create connector, create connect, delete connect, delete connector */
    @Test
    public void testConnectorConnectConnectConnector() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnector and wait till it's ready
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
        assertThat(runningConnectors.keySet(), is(empty()));

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        // triggered at least two times (Connect creation, Connector Status update)
        verify(api, atLeast(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));

        boolean connectDeleted = Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        assertThat(connectDeleted, is(true));
        waitForConnectorNotReady(connectorName,
                "NoSuchResourceException", "KafkaConnect resource 'cluster' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace ns.");

        boolean connectorDeleted = Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        assertThat(connectorDeleted, is(true));
        // Verify the connector was never deleted from connect as the cluster was deleted first
        verify(api, never()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Change the cluster label from one cluster to another
     * check the connector is deleted from the old cluster
     * check the connector is added to the new cluster
     * */
    @Test
    public void testChangeStrimziClusterLabel(VertxTestContext context) throws InterruptedException {
        String oldConnectClusterName = "cluster1";
        String newConnectClusterName = "cluster2";
        String connectorName = "connector";

        // Create two connect clusters
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(oldConnectClusterName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(oldConnectClusterName);

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(newConnectClusterName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(newConnectClusterName);

        // Create KafkaConnector associated with the first cluster using the Strimzi Cluster label and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, oldConnectClusterName)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec()
                .done();
        waitForConnectorReady(connectorName);

        // triggered twice (Connect creation, Connector Status update) for the first cluster
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(oldConnectClusterName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        // never triggered for the second cluster as connector's Strimzi cluster label does not match cluster 2
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(newConnectClusterName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // patch connector with new Strimzi cluster label associated with cluster 2
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).patch(new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName(connectorName)
                    .withNamespace(NAMESPACE)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, newConnectClusterName)
                .endMetadata()
                .withNewSpec()
                    .withTasksMax(1)
                    .withClassName("Dummy")
                .endSpec()
                .build());
        waitForConnectorReady(connectorName);

        // Note: The connector does not get deleted immediately from the first cluster, only on the next timed reconciliation

        verify(api, never()).delete(
                eq(KafkaConnectResources.qualifiedServiceName(oldConnectClusterName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(newConnectClusterName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Force reconciliation to assert connector deletion request occurs for first cluster
        Checkpoint async = context.checkpoint();
        kafkaConnectOperator.reconcile(new Reconciliation("test", "KafkaConnect", NAMESPACE, oldConnectClusterName))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(api, times(1)).delete(
                        eq(KafkaConnectResources.qualifiedServiceName(oldConnectClusterName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                        eq(connectorName));
                async.flag();
            })));
    }

    /** Create connect, create connector, delete connector, delete connect */
    @Test
    public void testConnectorNotReadyWhenExceptionFromConnectRestApi() {
        String connectName = "cluster";
        String connectorName = "connector";

        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any()))
            .thenAnswer(invocation -> Future.failedFuture(new ConnectRestException("GET", "/foo", 500, "Internal server error", "Bad stuff happened")));
        // NOTE: Clear runningConnectors as re-mocking it causes an entry to be added
        runningConnectors.clear();


        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered atleast once (Connect creation)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnector, should not go ready
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

        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(empty()));
    }

    /** Create connect, create connector, pause connector, resume connector */
    @Test
    public void testConnectorPauseResume() {
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
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (Connect creation, Connector Status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnector and wait till it's ready
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

        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));

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

        verify(api, times(1)).pause(
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

        verify(api, times(1)).pause(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, times(1)).resume(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Create connect, create connector, Scale to 0 */
    @Test
    public void testConnectScaleToZero() {
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
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));

        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnector and wait till it's ready
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

        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));

        when(api.list(any(), anyInt())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.listConnectorPlugins(any(), anyInt())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.getConnectorConfig(any(), any(), anyInt(), any())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.getConnector(any(), anyInt(), any())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).edit()
                .editSpec()
                    .withReplicas(0)
                .endSpec()
                .done();

        waitForConnectReady(connectName);
        waitForConnectorNotReady(connectorName, "RuntimeException", "Kafka Connect cluster 'cluster' in namespace ns has 0 replicas.");
    }

    /** Create connect, create connector, break the REST API */
    @Test
    public void testConnectRestAPIIssues() {
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
                    .withReplicas(1)
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // triggered twice (creation followed by status update)
        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));

        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnector and wait till it's ready
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

        verify(api, times(2)).list(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, times(2)).createOrUpdatePutRequest(
                eq(KafkaConnectResources.qualifiedServiceName(connectName, NAMESPACE)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertThat(runningConnectors.keySet(), is(Collections.singleton(key("cluster-connect-api.ns.svc", connectorName))));

        when(api.list(any(), anyInt())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.listConnectorPlugins(any(), anyInt())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.getConnectorConfig(any(), any(), anyInt(), any())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));
        when(api.getConnector(any(), anyInt(), any())).thenReturn(Future.failedFuture(new ConnectTimeoutException("connection timed out")));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).edit()
                .editSpec()
                    .withNewTemplate()
                    .endTemplate()
                .endSpec()
                .done();

        // Wait for Status change due to the broker REST API
        waitForConnectNotReady(connectName, "ConnectTimeoutException", "connection timed out");
        waitForConnectorNotReady(connectorName, "ConnectTimeoutException", "connection timed out");
    }
}
