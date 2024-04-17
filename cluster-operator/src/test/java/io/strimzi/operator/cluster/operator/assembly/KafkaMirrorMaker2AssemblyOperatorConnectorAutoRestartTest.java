/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.connector.AutoRestartStatusBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.TestUtils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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
public class KafkaMirrorMaker2AssemblyOperatorConnectorAutoRestartTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final KafkaMirrorMaker2 MM2 = new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName("my-mm2")
                    .withNamespace("namespace")
                    .withLabels(TestUtils.map("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build())
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withSourceCluster("source")
                            .withTargetCluster("target")
                            .withNewSourceConnector()
                                .withTasksMax(1)
                                .withConfig(Map.of("replication.factor", "-1"))
                            .endSourceConnector()
                            .build())
                .endSpec()
            .build();
    private static final Map<String, Object> RUNNING_STATUS = Map.of("connector", Map.of("state", "RUNNING"));
    private static final Map<String, Object> FAILING_STATUS = Map.of("connector", Map.of("state", "FAILED"));
    private static final Map<String, String> EXPECTED_CONNECTOR_CONFIG = new HashMap<>();
    static {
        EXPECTED_CONNECTOR_CONFIG.put("connector.class", "org.apache.kafka.connect.mirror.MirrorSourceConnector");
        EXPECTED_CONNECTOR_CONFIG.put("name", "source->target.MirrorSourceConnector");
        EXPECTED_CONNECTOR_CONFIG.put("replication.factor", "-1");
        EXPECTED_CONNECTOR_CONFIG.put("source.cluster.alias", "source");
        EXPECTED_CONNECTOR_CONFIG.put("source.cluster.bootstrap.servers", "source:9092");
        EXPECTED_CONNECTOR_CONFIG.put("source.cluster.security.protocol", "PLAINTEXT");
        EXPECTED_CONNECTOR_CONFIG.put("target.cluster.alias", "target");
        EXPECTED_CONNECTOR_CONFIG.put("target.cluster.bootstrap.servers", "target:9092");
        EXPECTED_CONNECTOR_CONFIG.put("target.cluster.security.protocol", "PLAINTEXT");
        EXPECTED_CONNECTOR_CONFIG.put("tasks.max", "1");
    }

    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private KafkaConnectApi mockConnectApi(Map<String, Object> status)    {
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.list(any(), any(), anyInt())).thenReturn(Future.succeededFuture(new ArrayList<>()));
        when(mockConnectApi.getConnectorConfig(any(), any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(Future.succeededFuture(EXPECTED_CONNECTOR_CONFIG));
        when(mockConnectApi.status(any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(Future.succeededFuture(status));
        when(mockConnectApi.statusWithBackOff(any(), any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(Future.succeededFuture(status));
        when(mockConnectApi.getConnectorTopics(any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(Future.succeededFuture(List.of()));
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(Future.succeededFuture(Map.of("connector", Map.of("state", "RESTARTING"))));

        return mockConnectApi;
    }

    @Test
    public void testAutoRestartWhenDisabled(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(RUNNING_STATUS);
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, MM2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, MM2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses(), is(List.of()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndNotFailed(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(RUNNING_STATUS);

        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                            .endAutoRestart()
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mm2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, mm2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses(), is(List.of()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedFirstRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(FAILING_STATUS);

        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                            .endAutoRestart()
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mm2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, mm2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().size(), is(1));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getConnectorName(), is("source->target.MirrorSourceConnector"));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getCount(), is(1));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getLastRestartTimestamp(), is(notNullValue()));

                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedSecondRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(FAILING_STATUS);

        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                            .endAutoRestart()
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .editOrNewStatus()
                .withAutoRestartStatuses(new AutoRestartStatusBuilder()
                        .withConnectorName("source->target.MirrorSourceConnector")
                        .withCount(1)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
                        .build())
                .endStatus()
                .build();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mm2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, mm2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().size(), is(1));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getConnectorName(), is("source->target.MirrorSourceConnector"));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getCount(), is(2));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getLastRestartTimestamp(), is(not(mm2.getStatus().getAutoRestartStatuses().get(0).getLastRestartTimestamp())));

                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedTooEarlyForSecondRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(FAILING_STATUS);

        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                            .endAutoRestart()
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .editOrNewStatus()
                .withAutoRestartStatuses(new AutoRestartStatusBuilder()
                        .withConnectorName("source->target.MirrorSourceConnector")
                        .withCount(1)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
                        .build())
                .endStatus()
                .build();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mm2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, mm2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                // This is failing because the connector is failing and was not restarted
                .onComplete(context.failing(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().size(), is(1));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getConnectorName(), is("source->target.MirrorSourceConnector"));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getCount(), is(1));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getLastRestartTimestamp(), is(mm2.getStatus().getAutoRestartStatuses().get(0).getLastRestartTimestamp()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartReset(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(RUNNING_STATUS);

        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                            .endAutoRestart()
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .editOrNewStatus()
                .withAutoRestartStatuses(new AutoRestartStatusBuilder()
                        .withConnectorName("source->target.MirrorSourceConnector")
                        .withCount(2)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(8).format(DateTimeFormatter.ISO_INSTANT))
                        .build())
                .endStatus()
                .build();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mm2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, mm2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().size(), is(0));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartTooEarlyForReset(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mockConnectApi(RUNNING_STATUS);

        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                            .endAutoRestart()
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .editOrNewStatus()
                .withAutoRestartStatuses(new AutoRestartStatusBuilder()
                        .withConnectorName("source->target.MirrorSourceConnector")
                        .withCount(2)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(5).format(DateTimeFormatter.ISO_INSTANT))
                        .build())
                .endStatus()
                .build();
        KafkaMirrorMaker2Cluster mirrorMaker2Cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mm2, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = new KafkaMirrorMaker2Status();

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), (vertx) -> mockConnectApi);

        Checkpoint checkpoint = context.checkpoint();

        op.reconcileConnectors(Reconciliation.DUMMY_RECONCILIATION, mm2, mirrorMaker2Cluster, kafkaMirrorMaker2Status)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().size(), is(1));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getConnectorName(), is("source->target.MirrorSourceConnector"));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getCount(), is(2));
                    assertThat(kafkaMirrorMaker2Status.getAutoRestartStatuses().get(0).getLastRestartTimestamp(), is(mm2.getStatus().getAutoRestartStatuses().get(0).getLastRestartTimestamp()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());

                    checkpoint.flag();
                })));
    }
}
