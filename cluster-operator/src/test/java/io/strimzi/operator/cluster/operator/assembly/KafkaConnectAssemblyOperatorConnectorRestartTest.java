/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator.STRIMZI_IO_RESTART_INCLUDE_TASKS_ARG;
import static io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator.STRIMZI_IO_RESTART_ONLY_FAILED_ARG;
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
public class KafkaConnectAssemblyOperatorConnectorRestartTest {
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

        KafkaConnector connector = buildKafkaConnector(STRIMZI_IO_RESTART_INCLUDE_TASKS_ARG + "," + STRIMZI_IO_RESTART_ONLY_FAILED_ARG);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), connector)
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

        KafkaConnector connector = buildKafkaConnector(STRIMZI_IO_RESTART_INCLUDE_TASKS_ARG);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), connector)
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

        KafkaConnector connector = buildKafkaConnector(STRIMZI_IO_RESTART_ONLY_FAILED_ARG);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), eq(false), eq(true));
                    checkpoint.flag();
                })));

    }

    @Test
    public void testConnectorRestartWhenInvalidArgs(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaConnector connector = buildKafkaConnector("true," + STRIMZI_IO_RESTART_ONLY_FAILED_ARG);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), eq(false), eq(false));
                    checkpoint.flag();
                })));

    }

    @Test
    public void testConnectorRestartWhenNoArgsPresent(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        configMock(supplier, mockConnectApi);

        KafkaConnector connector = buildKafkaConnector("true");

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.maybeCreateOrUpdateConnector(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), eq(false), eq(false));
                    checkpoint.flag();
                })));

    }

    private KafkaConnector buildKafkaConnector(String restartAnnotationValue) {
        return new KafkaConnectorBuilder()
                .withNewMetadata()
                .withName("my-connector")
                .withNamespace("my-namespace")
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_RESTART, restartAnnotationValue))
                .endMetadata()
                .withNewSpec()
                .withClassName("MyClass")
                .withTasksMax(3)
                .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .build();
    }

    private void configMock(ResourceOperatorSupplier supplier, KafkaConnectApi mockConnectApi) {
        when(supplier.kafkaConnectorOperator.patchAsync(any(), any())).thenReturn(Future.succeededFuture(new KafkaConnector()));

        when(mockConnectApi.getConnectorConfig(any(), any(), any(), anyInt(), any())).thenReturn(
                CompletableFuture.completedFuture(Map.of("topic", "my-topic", "tasks.max", "3", "name", "my-connector", "connector.class", "MyClass")));
        when(mockConnectApi.createOrUpdatePutRequest(any(), any(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockConnectApi.getConnectorTopics(any(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Object> status = Map.of("connector", Map.of("state", "RUNNING"));
        when(mockConnectApi.statusWithBackOff(any(), any(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(status));
        when(mockConnectApi.status(any(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(status));
    }
}
