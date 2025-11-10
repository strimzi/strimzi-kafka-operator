/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectorOffsetsAnnotation;
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
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
public class KafkaConnectAssemblyOperatorConnectorOffsetsTest {
    private static final String OFFSETS_JSON = "{\"partition\":1,\"offset\":1}";
    private static final Map<String, String> STOPPED_STATE = Map.of("state", "STOPPED");
    private static final Map<String, Object> STOPPED_STATE_MAP = Map.of("connector", STOPPED_STATE);
    private static final String CONFIGMAP_NAME = "my-config-map";
    private static final String NAMESPACE = "my-namespace";
    private static final String CONNECTOR_NAME = "my-connector";
    private static final String CONNECT_HOSTNAME = "my-connect-host";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testNoAnnotation(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnector().build();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(0));

                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // List Offsets Tests
    ////////////////////////////////////

    @Test
    public void testListOffsetsMissingListOffsetsProperty(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation.list);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing property listOffsets"));

                    verify(mockConnectApi, never()).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsApiCallFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = listOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Rest API call failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Rest API call failed"));


                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsGetConfigMapFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = listOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.failedFuture(new RuntimeException("Failed to get ConfigMap")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Failed to get ConfigMap"));

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(supplier.configMapOperations, never()).reconcile(any(), any(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsCreateOrUpdateConfigMapFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = listOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Create or update ConfigMap failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Create or update ConfigMap failed"));

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(any(), any(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsRemoveAnnotationFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = listOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Patch CR failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ListOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Patch CR failed"));

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(any(), any(), any(), any());
                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsSucceeds(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = listOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;



        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    ArgumentCaptor<ConfigMap> configMapArgumentCaptor = ArgumentCaptor.forClass(ConfigMap.class);
                    ArgumentCaptor<KafkaConnector> kafkaConnectorArgumentCaptor = ArgumentCaptor.forClass(KafkaConnector.class);

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(eq(reconciliation), eq(NAMESPACE), eq(CONFIGMAP_NAME), configMapArgumentCaptor.capture());
                    ConfigMap configMap = configMapArgumentCaptor.getValue();
                    assertThat(configMap.getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(configMap.getMetadata().getName(), is(CONFIGMAP_NAME));
                    assertThat(configMap.getMetadata().getLabels(), hasEntry("connector-label", "custom"));

                    List<OwnerReference> ownerReferenceList = configMap.getMetadata().getOwnerReferences();
                    assertThat(ownerReferenceList, hasSize(1));
                    assertThat(ownerReferenceList.get(0).getName(), is(CONNECTOR_NAME));
                    assertThat(ownerReferenceList.get(0).getKind(), is(Crds.kind(KafkaConnector.class)));
                    Map<String, String> configMapData = configMap.getData();
                    assertThat(configMapData, hasEntry("offsets.json", OFFSETS_JSON));

                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), kafkaConnectorArgumentCaptor.capture());
                    assertThat(kafkaConnectorArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));

                    context.completeNow();
                })));
    }

    @Test
    public void testListOffsetsDoesNotOverwriteExisting(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = listOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
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
                    .withOwnerReferences(ResourceUtils.DUMMY_OWNER_REFERENCE)
                    .endMetadata()
                .withData(existingData)
                .build();

        when(mockConnectApi.getConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(OFFSETS_JSON));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(existingCM));
        when(supplier.configMapOperations.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    ArgumentCaptor<ConfigMap> configMapArgumentCaptor = ArgumentCaptor.forClass(ConfigMap.class);
                    ArgumentCaptor<KafkaConnector> kafkaConnectorArgumentCaptor = ArgumentCaptor.forClass(KafkaConnector.class);

                    verify(mockConnectApi, times(1)).getConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).reconcile(eq(reconciliation), eq(NAMESPACE), eq(CONFIGMAP_NAME), configMapArgumentCaptor.capture());
                    ConfigMap configMap = configMapArgumentCaptor.getValue();
                    assertThat(configMap.getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(configMap.getMetadata().getName(), is(CONFIGMAP_NAME));

                    List<OwnerReference> ownerReferenceList = configMap.getMetadata().getOwnerReferences();
                    assertThat(ownerReferenceList, hasSize(1));
                    assertThat(ownerReferenceList.get(0).getName(), is("my-name"));
                    assertThat(configMap.getMetadata().getLabels(), hasEntry("label1", "value1"));
                    assertThat(configMap.getMetadata().getLabels(), hasEntry("connector-label", "custom"));
                    Map<String, String> configMapData = configMap.getData();
                    assertThat(configMapData, hasEntry("offsets.json", OFFSETS_JSON));
                    assertThat(configMapData, hasEntry("data1", "value1"));

                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), kafkaConnectorArgumentCaptor.capture());
                    assertThat(kafkaConnectorArgumentCaptor.getValue().getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // Alter Offsets Tests
    ////////////////////////////////////

    @Test
    public void testAlterOffsetsMissingAlterOffsetsProperty(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation.alter);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        op.manageConnectorOffsets(Reconciliation.DUMMY_RECONCILIATION, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("missing property alterOffsets"));

                    verify(mockConnectApi, never()).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsStatusApiCallFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Rest API call failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Rest API call failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsStatusApiCallMissingState(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(Map.of("foo", "bar")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("JSON response lacked $.connector.state"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, never()).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsGetConfigMapFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.failedFuture(new RuntimeException("Failed to get ConfigMap")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Failed to get ConfigMap"));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsConfigMapMissing(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(null));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("ConfigMap " + NAMESPACE + "/" + CONFIGMAP_NAME + " does not exist"));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsCMMissingData(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of("foo", "bar"))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Data field offsets.json is missing"));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsCMDataInvalid(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of("offsets.json", "{\"test\":}"))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Failed to parse contents of offsets.json as JSON"));


                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, never()).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsRemoveAnnotationFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of("offsets.json", OFFSETS_JSON))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));
        when(mockConnectApi.alterConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME, OFFSETS_JSON)).thenReturn(CompletableFuture.completedFuture(null));
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Patch CR failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("AlterOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Patch CR failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, times(1)).alterConnectorOffsets(any(), any(), anyInt(), any(), any());
                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testAlterOffsetsSucceeds(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = alterOffsetsKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        ConfigMap alterOffsetsConfigMap = new ConfigMapBuilder()
                .withData(Map.of("offsets.json", OFFSETS_JSON))
                .build();

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(supplier.configMapOperations.getAsync(NAMESPACE, CONFIGMAP_NAME)).thenReturn(Future.succeededFuture(alterOffsetsConfigMap));
        when(mockConnectApi.alterConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME, OFFSETS_JSON)).thenReturn(CompletableFuture.completedFuture(null));
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(supplier.configMapOperations, times(1)).getAsync(any(), any());
                    verify(mockConnectApi, times(1)).alterConnectorOffsets(any(), any(), anyInt(), any(), any());

                    ArgumentCaptor<KafkaConnector> kafkaConnectorArgumentCaptor = ArgumentCaptor.forClass(KafkaConnector.class);
                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), kafkaConnectorArgumentCaptor.capture());
                    KafkaConnector kafkaConnector = kafkaConnectorArgumentCaptor.getValue();
                    assertThat(kafkaConnector.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));

                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // Reset Offsets Tests
    ////////////////////////////////////

    @Test
    public void testResetOffsetsStatusApiCallFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation.reset);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Rest API call failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ResetOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Rest API call failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testResetOffsetsStatusApiCallMissingState(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation.reset);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(Map.of("foo", "bar")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ResetOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("JSON response lacked $.connector.state"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, never()).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.kafkaConnectorOperator, never()).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testResetOffsetsRemoveAnnotationFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation.reset);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(mockConnectApi.resetConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(null));
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Patch CR failed")));

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, hasSize(1));
                    Condition warningCondition = result.get(0);
                    assertThat(warningCondition.getReason(), is("ResetOffsets"));
                    assertThat(warningCondition.getMessage(), containsString("Patch CR failed"));

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, times(1)).resetConnectorOffsets(any(), any(), anyInt(), any());
                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), any());

                    context.completeNow();
                })));
    }

    @Test
    public void testResetOffsetsSucceeds(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        KafkaConnector connector = kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation.reset);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = Reconciliation.DUMMY_RECONCILIATION;

        when(mockConnectApi.status(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(STOPPED_STATE_MAP));
        when(mockConnectApi.resetConnectorOffsets(reconciliation, CONNECT_HOSTNAME, KafkaConnectCluster.REST_API_PORT, CONNECTOR_NAME)).thenReturn(CompletableFuture.completedFuture(null));
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.succeededFuture());

        op.manageConnectorOffsets(reconciliation, CONNECT_HOSTNAME, mockConnectApi, CONNECTOR_NAME, connector, connector.getSpec(), new ArrayList<>())
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    assertThat(result, empty());

                    verify(mockConnectApi, times(1)).status(any(), any(), anyInt(), any());
                    verify(mockConnectApi, times(1)).resetConnectorOffsets(any(), any(), anyInt(), any());
                    ArgumentCaptor<KafkaConnector> kafkaConnectorArgumentCaptor = ArgumentCaptor.forClass(KafkaConnector.class);
                    verify(supplier.kafkaConnectorOperator, times(1)).patchAsync(any(), kafkaConnectorArgumentCaptor.capture());
                    KafkaConnector kafkaConnector = kafkaConnectorArgumentCaptor.getValue();
                    assertThat(kafkaConnector.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));


                    context.completeNow();
                })));
    }

    ////////////////////////////////////
    // Utility Methods
    ////////////////////////////////////

    private KafkaConnectorBuilder kafkaConnector() {
        return new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName(CONNECTOR_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec();
    }

    private KafkaConnector kafkaConnectorWithAnnotation(KafkaConnectorOffsetsAnnotation offsetsAnnotation) {
        return kafkaConnector()
                .editMetadata()
                    .withAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, offsetsAnnotation.toString()))
                .endMetadata()
                .build();
    }

    private KafkaConnector listOffsetsKafkaConnector() {
        return kafkaConnector()
                .editMetadata()
                    .withLabels(Map.of("connector-label", "custom"))
                    .withAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.list.toString()))
                .endMetadata()
                .editSpec()
                    .withNewListOffsets()
                        .withNewToConfigMap(CONFIGMAP_NAME)
                    .endListOffsets()
                .endSpec()
                .build();
    }

    private KafkaConnector alterOffsetsKafkaConnector() {
        return kafkaConnector()
                .editMetadata()
                    .withAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.alter.toString()))
                .endMetadata()
                .editSpec()
                    .withNewAlterOffsets()
                        .withNewFromConfigMap(CONFIGMAP_NAME)
                    .endAlterOffsets()
                .endSpec()
                .build();
    }
}
