/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
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

import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerKRaftMigrationTest {

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final int REPLICAS = 3;
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static Vertx vertx;

    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled"))
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName("plain")
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .build())
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(REPLICAS)
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build();

    private final static KafkaNodePool CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(REPLICAS)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();

    private final static KafkaNodePool BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(REPLICAS)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            RECONCILIATION,
            CERT_MANAGER,
            PASSWORD_GENERATOR,
            CLUSTER_NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
    );

    private final static ClientsCa CLIENTS_CA = new ClientsCa(
            RECONCILIATION,
            new OpenSslCertManager(),
            PASSWORD_GENERATOR,
            KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()),
            365,
            30,
            true,
            null
    );

    @BeforeAll
    public static void beforeAll()  {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void afterAll()    {
        vertx.close();
    }

    @Test
    public void testMigrationFromZooKeeperToKRaftPostMigrationState(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KafkaMetadataState.ZooKeeper)
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(RECONCILIATION, kafka, CO_CONFIG.featureGates().useKRaftEnabled());

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

        KafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                kafka,
                List.of(BROKERS, CONTROLLERS),
                supplier,
                versionChange,
                kafkaMetadataStateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.KRaftMigration);
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            // Migration not completed so still in KraftMigration state
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.KRaftMigration);
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.KRaftDualWriting);
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.KRaftPostMigration);
            async.flag();
        }));
    }

    @Test
    public void testMigrationFromKRaftPostMigrationToKRaft(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KafkaMetadataState.KRaftPostMigration)
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(RECONCILIATION, kafka, CO_CONFIG.featureGates().useKRaftEnabled());

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

        KafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                kafka,
                List.of(BROKERS, CONTROLLERS),
                supplier,
                versionChange,
                kafkaMetadataStateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.PreKRaft);
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.KRaft);
            async.flag();
        }));
    }

    @Test
    public void testRollbackFromKRaftPostMigrationToKRaftDualWriting(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                .withKafkaMetadataState(KafkaMetadataState.KRaftPostMigration)
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(RECONCILIATION, kafka, CO_CONFIG.featureGates().useKRaftEnabled());

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

        KafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                kafka,
                List.of(BROKERS, CONTROLLERS),
                supplier,
                versionChange,
                kafkaMetadataStateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.KRaftDualWriting);
            async.flag();
        }));
    }

    @Test
    public void testRollbackFromKRaftDualWritingToZooKeeper(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KafkaMetadataState.KRaftDualWriting)
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(RECONCILIATION, kafka, CO_CONFIG.featureGates().useKRaftEnabled());

        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

        KafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                kafka,
                List.of(BROKERS),
                supplier,
                versionChange,
                kafkaMetadataStateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertEquals(status.getKafkaMetadataState(), KafkaMetadataState.ZooKeeper);
            async.flag();
        }));
    }

    static class MockKafkaReconciler extends KafkaReconciler {
        private static int count = 0;

        public MockKafkaReconciler(Reconciliation reconciliation, Kafka kafkaCr, List<KafkaNodePool> nodePools, ResourceOperatorSupplier supplier, KafkaVersionChange versionChange, KafkaMetadataStateManager kafkaMetadataStateManager) {
            super(reconciliation, kafkaCr, nodePools, createKafkaCluster(reconciliation, supplier, kafkaCr, nodePools, versionChange, kafkaMetadataStateManager), CLUSTER_CA, CLIENTS_CA, CO_CONFIG, supplier, PFA, vertx, kafkaMetadataStateManager);
        }

        private static KafkaCluster createKafkaCluster(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr, List<KafkaNodePool> nodePools, KafkaVersionChange versionChange, KafkaMetadataStateManager kafkaMetadataStateManager) {
            return KafkaClusterCreator.createKafkaCluster(
                    reconciliation,
                    kafkaCr,
                    nodePools,
                    Map.of(),
                    Map.of(),
                    versionChange,
                    kafkaMetadataStateManager.getMetadataConfigurationState(),
                    VERSIONS,
                    supplier.sharedEnvironmentProvider);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock) {
            return updateKafkaMetadataMigrationState()
                    .compose(i -> updateKafkaMetadataState(kafkaStatus));
        }
    }
}
