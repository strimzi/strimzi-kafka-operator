/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connector.AlterOffsets;
import io.strimzi.api.kafka.model.connector.AlterOffsetsBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorOffsetsAnnotation;
import io.strimzi.api.kafka.model.connector.ListOffsets;
import io.strimzi.api.kafka.model.connector.ListOffsetsBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaMirrorMaker2AssemblyOperatorConnectorOffsetsTest {
    private static final String OFFSETS_JSON = "{\"partition\":1,\"offset\":1}";
    private static final Map<String, String> STOPPED_STATE = Map.of("state", "STOPPED");
    private static final Map<String, Object> STOPPED_STATE_MAP = Map.of("connector", STOPPED_STATE);
    private static final String CONFIGMAP_NAME = "my-config-map";
    private static final String NAMESPACE = "my-namespace";
    private static final String MM2_NAME = "my-mm2";
    private static final String SOURCE_CLUSTER_ALIAS = "source";
    private static final String TARGET_CLUSTER_ALIAS = "target";
    private static final String CONNECT_HOSTNAME = "my-connect-host";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    protected static Vertx vertx;

    private static final String SOURCE_CONNECTOR = "MirrorSourceConnector";
    private static final String CHECKPOINT_CONNECTOR = "MirrorCheckpointConnector";
    private static final String HEARTBEAT_CONNECTOR = "MirrorHeartbeatConnector";

    private static String getConnectorName(String connector) {
        return SOURCE_CLUSTER_ALIAS + "->" + TARGET_CLUSTER_ALIAS + "." + connector;
    }

    private static String getConfigmapEntryName(String connector) {
        return SOURCE_CLUSTER_ALIAS + "--" + TARGET_CLUSTER_ALIAS + "." + connector + ".json";
    }

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private static Stream<Arguments> connectors() {
        return Stream.of(
                Arguments.of(SOURCE_CONNECTOR),
                Arguments.of(CHECKPOINT_CONNECTOR),
                Arguments.of(HEARTBEAT_CONNECTOR)
        );
    }

    @Test
    public void testNoAnnotation(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Builder().build();
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(0));

                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testMissingConnectorOffsetsAnnotation(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(null, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ManageOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing annotation strimzi.io/connector-offsets"));


                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // List Offsets Tests
    ////////////////////////////////////

    @Test
    public void testListOffsetsMissingListOffsetsProperty(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.list, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing property listOffsets"));

                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsMissingConnectorNameAnnotation(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(null);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ManageOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing annotation strimzi.io/mirrormaker-connector"));

                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsApiCallFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.failedFuture(new RuntimeException("Rest API call failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Rest API call failed"));


                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsGetConfigMapFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.failedFuture(new RuntimeException("Failed to get ConfigMap")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Failed to get ConfigMap"));

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsCreateOrUpdateConfigMapFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Create or update ConfigMap failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Create or update ConfigMap failed"));

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsRemoveAnnotationFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Patch CR failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Patch CR failed"));

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(any(), any(), any(), any());
                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @ParameterizedTest
    @MethodSource("connectors")
    public void testListOffsetsSucceeds(String connector, VertxTestContext context) {
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    ArgumentCaptor<ConfigMap> configMapArgumentCaptor = ArgumentCaptor.forClass(ConfigMap.class);
                    ArgumentCaptor<KafkaMirrorMaker2> kafkaMirrorMaker2ArgumentCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(eq(reconciliation), eq(NAMESPACE), eq(CONFIGMAP_NAME), configMapArgumentCaptor.capture());
                    ConfigMap configMap = configMapArgumentCaptor.getValue();
                    assertThat(configMap.getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(configMap.getMetadata().getName(), is(CONFIGMAP_NAME));
                    assertThat(configMap.getMetadata().getLabels(), hasEntry("mm2-label", "custom"));

                    List<OwnerReference> ownerReferenceList = configMap.getMetadata().getOwnerReferences();
                    assertThat(ownerReferenceList, hasSize(1));
                    assertThat(ownerReferenceList.get(0).getName(), is(MM2_NAME));
                    assertThat(ownerReferenceList.get(0).getKind(), is(Crds.kind(KafkaMirrorMaker2.class)));
                    Map<String, String> configMapData = configMap.getData();
                    assertThat(configMapData, hasEntry(getConfigmapEntryName(connector), OFFSETS_JSON));

                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), kafkaMirrorMaker2ArgumentCaptor.capture());
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    context.completeNow();
                })));
    }

    @ParameterizedTest
    @MethodSource("connectors")
    public void testListOffsetsDoesNotOverwriteExisting(String connector, VertxTestContext context) {
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = listOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        Map<String, String> existingCMLabels = new HashMap<>();
        existingCMLabels.put("label1", "value1");
        Map<String, String> existingData = new HashMap<>();
        existingData.put("data1", "value1");
        ConfigMap existingCM = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CONFIGMAP_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(existingCMLabels)
                .withOwnerReferences(new OwnerReferenceBuilder()
                        .withName("foo")
                        .build())
                .endMetadata()
                .withData(existingData)
                .build();

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(existingCM));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    ArgumentCaptor<ConfigMap> configMapArgumentCaptor = ArgumentCaptor.forClass(ConfigMap.class);
                    ArgumentCaptor<KafkaMirrorMaker2> kafkaMirrorMaker2ArgumentCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(eq(reconciliation), eq(NAMESPACE), eq(CONFIGMAP_NAME), configMapArgumentCaptor.capture());
                    ConfigMap configMap = configMapArgumentCaptor.getValue();
                    assertThat(configMap.getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(configMap.getMetadata().getName(), is(CONFIGMAP_NAME));
                    assertThat(configMap.getMetadata().getLabels(), hasEntry("label1", "value1"));
                    assertThat(configMap.getMetadata().getLabels(), hasEntry("mm2-label", "custom"));

                    List<OwnerReference> ownerReferenceList = configMap.getMetadata().getOwnerReferences();
                    assertThat(ownerReferenceList, hasSize(1));
                    assertThat(ownerReferenceList.get(0).getName(), is("foo"));
                    Map<String, String> configMapData = configMap.getData();
                    assertThat(configMapData, hasEntry(getConfigmapEntryName(connector), OFFSETS_JSON));
                    assertThat(configMapData, hasEntry("data1", "value1"));

                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), kafkaMirrorMaker2ArgumentCaptor.capture());
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // Alter Offsets Tests
    ////////////////////////////////////

    @Test
    public void testAlterOffsetsMissingAlterOffsetsProperty(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.alter, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing property alterOffsets"));

                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsMissingConnectorNameAnnotation(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(null);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ManageOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing annotation strimzi.io/mirrormaker-connector"));

                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsStatusApiCallFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.failedFuture(new RuntimeException("Rest API call failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Rest API call failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsStatusApiCallMissingState(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(Map.of("foo", "bar")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("JSON response lacked $.connector.state"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsGetConfigMapFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.failedFuture(new RuntimeException("Failed to get ConfigMap")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Failed to get ConfigMap"));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsConfigMapMissing(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("ConfigMap " + NAMESPACE + "/" + CONFIGMAP_NAME + " does not exist"));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsCMMissingData(VertxTestContext context) {
        String connector = SOURCE_CONNECTOR;
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of("foo", "bar"))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString(String.format("Data field %s is missing", getConfigmapEntryName(connector))));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsCMDataInvalid(VertxTestContext context) {
        String connector = SOURCE_CONNECTOR;
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of(getConfigmapEntryName(connector), "{\"test\":}"))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString(String.format("Failed to parse contents of %s as JSON", getConfigmapEntryName(connector))));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsRemoveAnnotationFails(VertxTestContext context) {
        String connector = SOURCE_CONNECTOR;
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of(getConfigmapEntryName(connector), OFFSETS_JSON))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));
        when(mockConnectApi.alterConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName, OFFSETS_JSON)).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Patch CR failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Patch CR failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, times(1)).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @ParameterizedTest
    @MethodSource("connectors")
    public void testAlterOffsetsSucceeds(String connector, VertxTestContext context) {
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = alterOffsetsKafkaMirrorMaker2(connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of(getConfigmapEntryName(connector), OFFSETS_JSON))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));
        when(mockConnectApi.alterConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName, OFFSETS_JSON)).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, times(1)).alterConnectorOffsets(any(), any(), anyInt(), any(), any());

                    ArgumentCaptor<KafkaMirrorMaker2> kafkaMirrorMaker2ArgumentCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), kafkaMirrorMaker2ArgumentCaptor.capture());
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // Reset Offsets Tests
    ////////////////////////////////////

    @Test
    public void testResetOffsetsConnectorNameAnnotationMissing(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.reset, null);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(),  new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ManageOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing annotation strimzi.io/mirrormaker-connector"));

                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testResetOffsetsStatusApiCallFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.reset, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.failedFuture(new RuntimeException("Rest API call failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(),  new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ResetOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Rest API call failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testResetOffsetsStatusApiCallMissingState(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.reset, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(Map.of("foo", "bar")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ResetOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("JSON response lacked $.connector.state"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.mirrorMaker2Operator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testResetOffsetsRemoveAnnotationFails(VertxTestContext context) {
        String connectorName = getConnectorName(SOURCE_CONNECTOR);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.reset, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(mockConnectApi.resetConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Patch CR failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ResetOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Patch CR failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, times(1)).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @ParameterizedTest
    @MethodSource("connectors")
    public void testResetOffsetsSucceeds(String connector, VertxTestContext context) {
        String connectorName = getConnectorName(connector);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation.reset, connectorName);
        KafkaMirrorMaker2Cluster kafkaMirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaConnector kafkaMirrorMaker2SourceConnector = kafkaMirrorMaker2Cluster.connectors()
                .generateConnectorDefinitions()
                .stream()
                .filter(kafkaConnector -> connectorName.equals(kafkaConnector.getMetadata().getName()))
                .findFirst()
                .get();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture(STOPPED_STATE_MAP));
        when(mockConnectApi.resetConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, connectorName)).thenReturn(Future.succeededFuture());
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, connectorName, kafkaMirrorMaker2, kafkaMirrorMaker2SourceConnector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, times(1)).resetConnectorOffsets(any(), any(), anyInt(), any());

                    ArgumentCaptor<KafkaMirrorMaker2> kafkaMirrorMaker2ArgumentCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
                    verify(supplier.mirrorMaker2Operator, times(1)).patchAsync(any(), kafkaMirrorMaker2ArgumentCaptor.capture());
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2ArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // Utility Methods
    ////////////////////////////////////

    private KafkaMirrorMaker2Builder kafkaMirrorMaker2Builder() {
        return new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(MM2_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .withReplicas(1)
                .withConnectCluster("target")
                .withClusters(
                        new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                        new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build()
                ).withMirrors(
                        new KafkaMirrorMaker2MirrorSpecBuilder()
                                .withSourceCluster("source")
                                .withTargetCluster("target")
                                .withNewSourceConnector()
                                    .withTasksMax(1)
                                    .withConfig(Map.of("replication.factor", "-1"))
                                .endSourceConnector()
                                .withNewCheckpointConnector()
                                    .withTasksMax(1)
                                    .withConfig(Map.of("replication.factor", "-1"))
                                .endCheckpointConnector()
                                .withNewHeartbeatConnector()
                                    .withTasksMax(1)
                                    .withConfig(Map.of("replication.factor", "-1"))
                                .endHeartbeatConnector()
                                .build()
                ).endSpec();
    }

    private KafkaMirrorMaker2 kafkaMirrorMaker2WithAnnotations(KafkaConnectorOffsetsAnnotation offsetsAnnotation, String mirrorMakerConnector) {
        Map<String, String> annotations = new HashMap<>(2);
        if (offsetsAnnotation != null) {
            annotations.put(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, offsetsAnnotation.toString());
        }
        if (mirrorMakerConnector != null) {
            annotations.put(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, mirrorMakerConnector);
        }
        return kafkaMirrorMaker2Builder()
                .editMetadata()
                    .withAnnotations(annotations)
                .endMetadata()
                .build();
    }

    private KafkaMirrorMaker2 listOffsetsKafkaMirrorMaker2(String mirrorMakerConnector) {
        Map<String, String> annotations = new HashMap<>(2);
        annotations.put(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.list.toString());
        if (mirrorMakerConnector != null) {
            annotations.put(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, mirrorMakerConnector);
        }
        ListOffsets listOffsets = new ListOffsetsBuilder()
                .withNewToConfigMap(CONFIGMAP_NAME)
                .build();
        return kafkaMirrorMaker2Builder()
                .editMetadata()
                    .withLabels(Map.of("mm2-label", "custom"))
                    .withAnnotations(annotations)
                .endMetadata()
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .withListOffsets(listOffsets)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .withListOffsets(listOffsets)
                        .endCheckpointConnector()
                        .editHeartbeatConnector()
                            .withListOffsets(listOffsets)
                        .endHeartbeatConnector()
                    .endMirror()
                .endSpec()
                .build();
    }

    private KafkaMirrorMaker2 alterOffsetsKafkaMirrorMaker2(String mirrorMakerConnector) {
        Map<String, String> annotations = new HashMap<>(2);
        annotations.put(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.alter.toString());
        if (mirrorMakerConnector != null) {
            annotations.put(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, mirrorMakerConnector);
        }
        AlterOffsets alterOffsets = new AlterOffsetsBuilder()
                .withNewFromConfigMap(CONFIGMAP_NAME)
                .build();
        return kafkaMirrorMaker2Builder()
                .editMetadata()
                .withAnnotations(annotations)
                .endMetadata()
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .withAlterOffsets(alterOffsets)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .withAlterOffsets(alterOffsets)
                        .endCheckpointConnector()
                        .editHeartbeatConnector()
                            .withAlterOffsets(alterOffsets)
                        .endHeartbeatConnector()
                    .endMirror()
                .endSpec()
                .build();
    }
}
