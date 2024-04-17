/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectAssemblyOperatorConnectorAutoRestartTest {
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
    public void testAutoRestartWhenDisabled(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of(), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
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

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(nullValue()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, never()).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndNotFailed(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of(), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(nullValue()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedFirstRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(Future.succeededFuture(Map.of()));
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of("connector", Map.of("state", "FAILED")), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(1));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));

                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedSecondRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(Future.succeededFuture(Map.of()));
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of("connector", Map.of("state", "FAILED")), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(1)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(2));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(not(connector.getStatus().getAutoRestart().getLastRestartTimestamp()))); // Timestamp changed

                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedTooEarlyForSecondRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of("connector", Map.of("state", "FAILED")), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(1)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(1));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(connector.getStatus().getAutoRestart().getLastRestartTimestamp())); // Timestamp changed

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartReset(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of(), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(2)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(8).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(nullValue()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartTooEarlyForReset(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of(), List.of(), List.of(), null);
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(2)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(5).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(2));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(connector.getStatus().getAutoRestart().getLastRestartTimestamp())); // Timestamp changed

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }
}
