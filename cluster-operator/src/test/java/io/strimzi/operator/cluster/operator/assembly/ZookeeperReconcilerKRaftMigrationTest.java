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
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
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
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZookeeperReconcilerKRaftMigrationTest {

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

    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            RECONCILIATION,
            CERT_MANAGER,
            PASSWORD_GENERATOR,
            CLUSTER_NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
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
    public void testZookeeperReconcilerWithKRaftMigrationRollback(VertxTestContext context) {

        Kafka patchedKafka = new KafkaBuilder(KAFKA)
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
        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

        KafkaMetadataStateManager stateManager = new KafkaMetadataStateManager(RECONCILIATION, patchedKafka, CO_CONFIG.featureGates().useKRaftEnabled());

        MockZooKeeperReconciler zookeeperReconciler = spy(new MockZooKeeperReconciler(
                RECONCILIATION,
                vertx,
                CO_CONFIG,
                supplier,
                PFA,
                patchedKafka,
                versionChange,
                null,
                0,
                CLUSTER_CA,
                stateManager.isRollingBack()));

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();

        zookeeperReconciler.reconcile(status, Clock.systemUTC()).onComplete(context.succeeding(v -> context.verify(() -> {
            verify(zookeeperReconciler, times(1)).maybeDeleteControllerZnode();
            verify(zookeeperReconciler, times(1)).deleteControllerZnode();
            async.flag();
        })));
    }

    @Test
    public void testZookeeperReconcilerWithNoKRaftMigrationRollback(VertxTestContext context) {

        Kafka patchedKafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KafkaMetadataState.KRaft)
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        KafkaVersionChange versionChange = new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), VERSIONS.defaultVersion().messageVersion(), VERSIONS.defaultVersion().metadataVersion());

        KafkaMetadataStateManager stateManager = new KafkaMetadataStateManager(RECONCILIATION, patchedKafka, CO_CONFIG.featureGates().useKRaftEnabled());

        MockZooKeeperReconciler zookeeperReconciler = spy(new MockZooKeeperReconciler(
                RECONCILIATION,
                vertx,
                CO_CONFIG,
                supplier,
                PFA,
                patchedKafka,
                versionChange,
                null,
                0,
                CLUSTER_CA,
                stateManager.isRollingBack()));

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();

        zookeeperReconciler.reconcile(status, Clock.systemUTC()).onComplete(context.succeeding(res -> context.verify(() -> {
            verify(zookeeperReconciler, times(1)).maybeDeleteControllerZnode();
            verify(zookeeperReconciler, times(0)).deleteControllerZnode();
            async.flag();
        })));
    }

    static class MockZooKeeperReconciler extends ZooKeeperReconciler   {
        public MockZooKeeperReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, KafkaVersionChange versionChange, Storage oldStorage, int currentReplicas, ClusterCa clusterCa, boolean kraftMigrationRollback) {
            super(reconciliation, vertx, config, supplier, pfa, kafkaAssembly, versionChange, oldStorage, currentReplicas, clusterCa, kraftMigrationRollback);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return maybeDeleteControllerZnode();
        }

        @Override
        protected Future<Void> deleteControllerZnode() {
            return Future.succeededFuture();
        }
    }
}