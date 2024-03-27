/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
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
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerUpgradeDowngradeTest {
    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "testkafka";
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();
    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            CLUSTER_NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
    );
    private final static ClientsCa CLIENTS_CA = new ClientsCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()),
            365,
            30,
            true,
            null
    );
    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withConfig(new HashMap<>())
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

    private static Vertx vertx;

    @BeforeAll
    public static void beforeAll()  {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void afterAll()    {
        vertx.close();
    }

    @Test
    public void testWithAllVersionsInCR(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withVersion(VERSIONS.defaultVersion().version())
                        .addToConfig(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, "2.8")
                        .addToConfig(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, "2.8")
                    .endKafka()
                .endSpec()
                .build();

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, null);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock StrimziPodSet operations
        StrimziPodSetOperator mockSpsOps = supplier.strimziPodSetOperator;
        when(mockSpsOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockSpsOps.batchReconcile(any(), any(), any(), any())).thenCallRealMethod();
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockSpsOps.reconcile(any(), any(), any(), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new StrimziPodSet())));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka,
                versionChange
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(context.succeeding(i -> context.verify(() -> {
            assertThat(spsCaptor.getAllValues().size(), is(1));

            StrimziPodSet sps = spsCaptor.getValue();

            assertThat(sps.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(VERSIONS.defaultVersion().version()));
            sps.getSpec().getPods().forEach(map -> {
                Pod pod = PodSetUtils.mapToPod(map);

                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(VERSIONS.defaultVersion().version()));
                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION), is("2.8"));
                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION), is("2.8"));
            });

            async.flag();
        })));
    }

    @Test
    public void testWithoutAnyVersionsInCR(VertxTestContext context) {
        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), null);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock StrimziPodSet operations
        StrimziPodSetOperator mockSpsOps = supplier.strimziPodSetOperator;
        when(mockSpsOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockSpsOps.batchReconcile(any(), any(), any(), any())).thenCallRealMethod();
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockSpsOps.reconcile(any(), any(), any(), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new StrimziPodSet())));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                KAFKA,
                versionChange
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(context.succeeding(i -> context.verify(() -> {
            assertThat(spsCaptor.getAllValues().size(), is(1));

            StrimziPodSet sps = spsCaptor.getValue();

            assertThat(sps.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(VERSIONS.defaultVersion().version()));
            sps.getSpec().getPods().forEach(map -> {
                Pod pod = PodSetUtils.mapToPod(map);

                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(VERSIONS.defaultVersion().version()));
                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION), is(VERSIONS.defaultVersion().protocolVersion()));
                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION), is(VERSIONS.defaultVersion().messageVersion()));
            });

            async.flag();
        })));
    }

    @Test
    public void testUpgradingWithSpecificProtocolAndMessageFormatVersions(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withVersion(VERSIONS.defaultVersion().version())
                        .addToConfig(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, VERSIONS.defaultVersion().protocolVersion())
                        .addToConfig(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, VERSIONS.defaultVersion().messageVersion())
                    .endKafka()
                .endSpec()
                .build();

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), VERSIONS.defaultVersion(), KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION, null);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock StrimziPodSet operations
        StrimziPodSetOperator mockSpsOps = supplier.strimziPodSetOperator;
        when(mockSpsOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockSpsOps.batchReconcile(any(), any(), any(), any())).thenCallRealMethod();
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockSpsOps.reconcile(any(), any(), any(), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new StrimziPodSet())));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka,
                versionChange
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(context.succeeding(i -> context.verify(() -> {
            assertThat(spsCaptor.getAllValues().size(), is(1));

            StrimziPodSet sps = spsCaptor.getValue();

            assertThat(sps.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(VERSIONS.defaultVersion().version()));
            sps.getSpec().getPods().forEach(map -> {
                Pod pod = PodSetUtils.mapToPod(map);

                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(VERSIONS.defaultVersion().version()));
                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION), is(KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION));
                assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION), is(KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION));
            });

            async.flag();
        })));
    }

    static class MockKafkaReconciler extends KafkaReconciler {
        public MockKafkaReconciler(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr, KafkaVersionChange versionChange) {
            super(reconciliation, kafkaCr, null, createKafkaCluster(reconciliation, supplier, kafkaCr, versionChange), CLUSTER_CA, CLIENTS_CA, CO_CONFIG, supplier, PFA, vertx, new KafkaMetadataStateManager(reconciliation, kafkaCr, CO_CONFIG.featureGates().useKRaftEnabled()));
            listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
        }

        private static KafkaCluster createKafkaCluster(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr, KafkaVersionChange versionChange)   {
            return  KafkaClusterCreator.createKafkaCluster(
                    reconciliation,
                    kafkaCr,
                    null,
                    Map.of(),
                    Map.of(),
                    versionChange,
                    new KafkaMetadataStateManager(reconciliation, kafkaCr, CO_CONFIG.featureGates().useKRaftEnabled()).getMetadataConfigurationState(),
                    VERSIONS,
                    supplier.sharedEnvironmentProvider);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return podSet().map((Void) null);
        }
    }
}
