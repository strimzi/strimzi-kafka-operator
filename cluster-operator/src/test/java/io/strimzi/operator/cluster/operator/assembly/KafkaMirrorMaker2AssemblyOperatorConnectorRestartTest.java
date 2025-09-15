/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator.STRIMZI_IO_RESTART_INCLUDE_TASKS_ARG;
import static io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator.STRIMZI_IO_RESTART_ONLY_FAILED_ARG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaMirrorMaker2AssemblyOperatorConnectorRestartTest {

    /**
     * Helper class to represent a request to test an annotation value. Important to note that method to check if we have a
     * restart request is different from method to check if the annotation value is valid.
     *
     * @param annotationValue The value of the annotation
     * @param connectorName The name of the connector to restart
     * @param shouldRestart Whether the operator should interpret the annotation as a restart request
     * @param shouldBeValid Whether the operator should interpret the annotation as a valid value
     * */
    record AnnotationValueRequest(String annotationValue, String connectorName,  boolean shouldRestart, boolean shouldBeValid) { }


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
    public void testConnectorRestartWhenAllArgsPresent(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Builder(STRIMZI_IO_RESTART_INCLUDE_TASKS_ARG + "," + STRIMZI_IO_RESTART_ONLY_FAILED_ARG).build();
        KafkaConnector connector = buildKafkaConnector();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), kafkaMirrorMaker2)
                        .onComplete(context.succeeding(r -> context.verify(() -> {
                            verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), eq(true), eq(true));
                            checkpoint.flag();
                        })));

    }

    @Test
    public void testConnectorRestartWhenIncludeTasksPresent(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Builder(STRIMZI_IO_RESTART_INCLUDE_TASKS_ARG).build();
        KafkaConnector connector = buildKafkaConnector();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), kafkaMirrorMaker2)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), eq(true), eq(false));
                    checkpoint.flag();
                })));

    }

    @Test
    public void testConnectorRestartWhenOnlyFailedPresent(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Builder(STRIMZI_IO_RESTART_ONLY_FAILED_ARG).build();
        KafkaConnector connector = buildKafkaConnector();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), kafkaMirrorMaker2)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), eq(false), eq(true));
                    checkpoint.flag();
                })));

    }

    @Test
    public void testConnectorRestartWhenNoArgsPresent(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Builder(null).build();
        KafkaConnector connector = buildKafkaConnector();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), kafkaMirrorMaker2)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), eq(false), eq(false));
                    checkpoint.flag();
                })));

    }

    @Test
    public void testConnectorRestartWhenInvalidArgs(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Builder("invalid").build();
        KafkaConnector connector = buildKafkaConnector();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), kafkaMirrorMaker2)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), eq(false), eq(false));
                    checkpoint.flag();
                })));

    }

    @Test
    public void testRestartAnnotationValuesIsValid() {
        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(null, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ResourceUtils.supplierWithMocks(false), ResourceUtils.dummyClusterOperatorConfig());
        for (AnnotationValueRequest annoValue : generateAnnotationValues()) {
            HasMetadata resource = new KafkaMirrorMaker2Builder()
                    .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR, annoValue.annotationValue))
                    .endMetadata()
                    .build();
            boolean isValid = op.restartAnnotationIsValid(resource, annoValue.connectorName);
            boolean shouldBeValid = annoValue.shouldBeValid;
            assertThat(String.format("value '%s' is expected to %s", annoValue, shouldBeValid ? "be valid" : "NOT be valid"), isValid, equalTo(shouldBeValid));
        }
    }

    @Test
    public void testRestartAnnotationValueHasRestart() {
        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(null, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ResourceUtils.supplierWithMocks(false), ResourceUtils.dummyClusterOperatorConfig());
        for (AnnotationValueRequest annoValue : generateAnnotationValues()) {
            HasMetadata resource = new KafkaMirrorMaker2Builder()
                    .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR, annoValue.annotationValue))
                    .endMetadata()
                    .build();
            boolean hasRestart = op.hasRestartAnnotation(resource, annoValue.connectorName);
            boolean shouldRestart = annoValue.shouldRestart;
            assertThat(String.format("value '%s' is expected to %s", annoValue, shouldRestart ? "restart" : "NOT restart"), hasRestart, equalTo(shouldRestart));
        }
    }

    private List<AnnotationValueRequest> generateAnnotationValues() {
        String connectorName = "my-connector";
        return List.of(
                new AnnotationValueRequest(connectorName, connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":includeTasks,onlyFailed", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":onlyFailed,includeTasks", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":    onlyFailed   ,   includeTasks   ", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":includeTasks", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":onlyFailed", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":   onlyFailed   ", connectorName, true, true),
                new AnnotationValueRequest(connectorName + ":    includeTasks   ", connectorName, true, true),
                new AnnotationValueRequest("    " + connectorName + ":    includeTasks   ", connectorName, true, false),
                new AnnotationValueRequest("", connectorName, false, false),
                new AnnotationValueRequest("     ", connectorName, false, false),
                new AnnotationValueRequest("   b  ", connectorName, false, false)
        );
    }

    private KafkaConnector buildKafkaConnector() {
        return new KafkaConnectorBuilder()
                    .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                    .endMetadata()
                    .withNewSpec()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                    .endSpec()
                    .build();
    }

    private void configMock(ResourceOperatorSupplier supplier, KafkaConnectApi mockConnectApi) {
        when(supplier.mirrorMaker2Operator.patchAsync(any(), any())).thenReturn(Future.succeededFuture(new KafkaMirrorMaker2()));

        when(mockConnectApi.getConnectorConfig(any(), any(), any(), anyInt(), any())).thenReturn(
                CompletableFuture.completedFuture(Map.of("topic", "my-topic", "tasks.max", "3", "name", "my-connector", "connector.class", "MyClass")));
        when(mockConnectApi.createOrUpdatePutRequest(any(), any(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockConnectApi.getConnectorTopics(any(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Object> status = Map.of("connector", Map.of("state", "RUNNING"));
        when(mockConnectApi.statusWithBackOff(any(), any(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(status));
        when(mockConnectApi.status(any(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(status));
    }

    private KafkaMirrorMaker2Builder kafkaMirrorMaker2Builder(String restartAnnotationValue) {
        return new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART_CONNECTOR, restartAnnotationValue != null ? "my-connector:" + restartAnnotationValue : "my-connector"))
                    .withName("test")
                    .withNamespace("test")
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
}
