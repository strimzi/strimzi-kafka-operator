/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerUpgradeDowngradeTest {
    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "testkafka";
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_22);
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

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock StrimziPodSet operations
        StrimziPodSetOperator mockSpsOps = supplier.strimziPodSetOperator;
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockSpsOps.reconcile(any(), any(), any(), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new StrimziPodSet())));

        // Mock Secret gets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.kafkaSecretName(CLUSTER_NAME))).thenReturn(
                Future.succeededFuture(ResourceUtils.createMockBrokersCertsSecret(NAMESPACE,
                        CLUSTER_NAME,
                        kafka.getSpec().getKafka().getReplicas(),
                        KafkaResources.kafkaSecretName(CLUSTER_NAME),
                        MockCertManager.serverCert(),
                        MockCertManager.serverKey(),
                        MockCertManager.serverKeyStore(),
                        MockCertManager.certStorePassword()
                ))
        );

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
        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock StrimziPodSet operations
        StrimziPodSetOperator mockSpsOps = supplier.strimziPodSetOperator;
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockSpsOps.reconcile(any(), any(), any(), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new StrimziPodSet())));

        // Mock Secret gets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.kafkaSecretName(CLUSTER_NAME))).thenReturn(
                Future.succeededFuture(ResourceUtils.createMockBrokersCertsSecret(NAMESPACE,
                        CLUSTER_NAME,
                        KAFKA.getSpec().getKafka().getReplicas(),
                        KafkaResources.kafkaSecretName(CLUSTER_NAME),
                        MockCertManager.serverCert(),
                        MockCertManager.serverKey(),
                        MockCertManager.serverKeyStore(),
                        MockCertManager.certStorePassword()
                ))
        );

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

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), VERSIONS.defaultVersion(), KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock StrimziPodSet operations
        StrimziPodSetOperator mockSpsOps = supplier.strimziPodSetOperator;
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockSpsOps.reconcile(any(), any(), any(), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new StrimziPodSet())));

        // Mock Secret gets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.kafkaSecretName(CLUSTER_NAME))).thenReturn(
                Future.succeededFuture(ResourceUtils.createMockBrokersCertsSecret(NAMESPACE,
                        CLUSTER_NAME,
                        kafka.getSpec().getKafka().getReplicas(),
                        KafkaResources.kafkaSecretName(CLUSTER_NAME),
                        MockCertManager.serverCert(),
                        MockCertManager.serverKey(),
                        MockCertManager.serverKeyStore(),
                        MockCertManager.certStorePassword()
                ))
        );

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
            super(reconciliation, kafkaCr, null, 0, CLUSTER_CA, CLIENTS_CA, versionChange, CO_CONFIG, supplier, PFA, vertx);
            listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return podSet().map((Void) null);
        }
    }
}
