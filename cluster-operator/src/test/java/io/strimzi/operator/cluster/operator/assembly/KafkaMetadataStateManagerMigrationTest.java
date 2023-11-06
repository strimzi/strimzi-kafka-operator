/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
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
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.KRaftMigrationState;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaft;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftDualWriting;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftMigration;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftPostMigration;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.PreKRaft;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.ZooKeeper;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaMetadataStateManagerMigrationTest {

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final ClusterOperatorConfig CONFIG = ResourceUtils.dummyClusterOperatorConfig();
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

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

    private static final Kafka KAFKA = new KafkaBuilder()
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
            .endSpec()
            .build();

    private static KafkaCluster createKafkaCluster(Reconciliation reconciliation, Kafka kafka, KafkaMetadataConfigurationState state)   {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(reconciliation, kafka, List.of(CONTROLLERS, BROKERS), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        return KafkaCluster.fromCrd(
                reconciliation,
                kafka,
                pools,
                VERSIONS,
                KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                state,
                null,
                SHARED_ENV_PROVIDER);
    }

    private final static KafkaNodePool CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"))).build())
            .endSpec()
            .build();

    private final static KafkaNodePool BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("6"))).build())
            .endSpec()
            .build();

    /**
     * Tests migration from Zookeeper State to KRaftPostMigration State.
     * The strimzi.io/kraft annotation is set to `migration`
     *
     * @param context Test context
     */
    @Test
    public void testMigrationFromZooKeeperToKRaftPostMigrationState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(ZooKeeper.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));
            assertEquals(status.getKafkaMetadataState(), KRaftMigration.name());
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            // Migration not completed so still in KraftMigration state
            assertEquals(status.getKafkaMetadataState(), KRaftMigration.name());
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            assertEquals(status.getKafkaMetadataState(), KRaftDualWriting.name());
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            assertEquals(status.getKafkaMetadataState(), KRaftPostMigration.name());
            async.flag();
        }));
    }

    /**
     * Tests migration from KRaftPostMigration State to KRaft State.
     * The strimzi.io/kraft annotation is set to `enabled`
     *
     * @param context Test Context
     */
    @Test
    public void testKRaftPostMigrationToKRaftState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftPostMigration.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));
            assertEquals(status.getKafkaMetadataState(), PreKRaft.name());
        })).compose(v -> kr.reconcile(status, Clock.systemUTC())).onComplete(context.succeeding(v -> {
            assertEquals(status.getKafkaMetadataState(), KRaft.name());
            async.flag();
        }));
    }

    /**
     * Tests rollback from KRaftPostMigration State to KRaftDualWriting State.
     * The strimzi.io/kraft annotation is set to `rollback`
     *
     * @param context Test Context
     */
    @Test
    public void testRollbackToKRaftDualWritingState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftPostMigration.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertEquals(status.getKafkaMetadataState(), KRaftDualWriting.name());
            async.flag();
        }));
    }

    /**
     * Tests migration from KRaftDualWriting State to Zookeeper State.
     * The strimzi.io/kraft annotation is set to `disabled`
     *
     * @param context Test Context
     */
    @Test
    public void testRollbackFromKafkaDualWritingToZooKeeperState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftDualWriting.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertEquals(status.getKafkaMetadataState(), ZooKeeper.name());
            async.flag();
        }));
    }

    /**
     * Tests warning in Kafka status if wrong annotation applied in KRaft State.
     *
     * @param context Test Context
     */
    @Test
    public void testWarningIfWrongAnnotationAppliedInKraftState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaft.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertTrue(status.getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
            assertEquals(status.getConditions().get(4).getMessage(), "The strimzi.io/kraft annotation can't be set to migration, rollback or disabled values because the cluster is already KRaft.");
            assertEquals(status.getKafkaMetadataState(), KRaft.name());
            async.flag();
        }));
    }

    /**
     * Tests warning in Kafka status if wrong annotation applied in Zookeeper State.
     *
     * @param context Test Context
     */
    @Test
    public void testWarningIfWrongAnnotationAppliedInZookeeperState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(ZooKeeper.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertTrue(status.getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
            assertEquals(status.getConditions().get(2).getMessage(), "The strimzi.io/kraft annotation can't be set to rollback because the cluster is already ZooKeeper-based." +
                    "There is no migration ongoing to rollback." +
                    "If you want to migrate it to be KRaft-based apply the migration value instead.");
            assertEquals(status.getKafkaMetadataState(), ZooKeeper.name());
            async.flag();
        }));
    }

    /**
     * Tests warning in Kafka status if wrong annotation applied in KraftPostMigration State.
     *
     * @param context Test Context
     */
    @Test
    public void testWarningIfWrongAnnotationAppliedInKraftPostMigrationState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftPostMigration.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertTrue(status.getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
            assertEquals(status.getConditions().get(2).getMessage(), "The strimzi.io/kraft annotation can't be set to migration or disabled in the post-migration." +
                    "You can use rollback value to come back to ZooKeeper. Use the enabled value to finalize migration instead.");
            assertEquals(status.getKafkaMetadataState(), KRaftPostMigration.name());
            async.flag();
        }));
    }

    /**
     * Tests warning in Kafka status if wrong annotation applied in KraftMigration State.
     *
     * @param context Test Context
     */
    @Test
    public void testWarningIfWrongAnnotationAppliedInKRaftMigrationState(VertxTestContext context) {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftMigration.name())
                .endStatus()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        MockKafkaMetadataStateManager stateManager = new MockKafkaMetadataStateManager(RECONCILIATION, kafka, CONFIG.featureGates().useKRaftEnabled());

        MockKafkaReconciler kr = new MockKafkaReconciler(
                RECONCILIATION,
                vertx,
                CONFIG,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(BROKERS, CONTROLLERS),
                CLUSTER_CA,
                CLIENTS_CA,
                stateManager);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertTrue(status.getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
            assertEquals(status.getConditions().get(2).getMessage(), "The strimzi.io/kraft annotation can't be set to rollback during a migration process." +
                    "It can be used in post migration to start rollback process.");
            assertEquals(status.getKafkaMetadataState(), KRaftMigration.name());
            async.flag();
        }));
    }

    /** Mock for KafkaMetadataStateManager class */
    static class MockKafkaMetadataStateManager extends KafkaMetadataStateManager {
        private static int count = 0;

        /**
         * Constructor
         *
         * @param reconciliation             Reconciliation information
         * @param kafkaCr                    instance of the Kafka CR
         * @param useKRaftFeatureGateEnabled if the UseKRaft feature gate is enabled on the operator
         */
        public MockKafkaMetadataStateManager(Reconciliation reconciliation, Kafka kafkaCr, boolean useKRaftFeatureGateEnabled) {
            super(reconciliation, kafkaCr, useKRaftFeatureGateEnabled);
        }

        @Override
        protected KafkaAgentClient initKafkaAgentClient(Reconciliation reconciliation, Secret clusterCaCertSecret, Secret coKeySecret) {
            KafkaAgentClient agentClient = mock(KafkaAgentClient.class);
            if (++count == 2) {
                when(agentClient.getKRaftMigrationState(any())).thenReturn(new KRaftMigrationState(1));
            } else {
                when(agentClient.getKRaftMigrationState(any())).thenReturn(new KRaftMigrationState(2));
            }
            return agentClient;
        }
    }

    /** Mock for KafkaReconciler class */
    static class MockKafkaReconciler extends KafkaReconciler {
        private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MockKafkaReconciler.class.getName());

        public MockKafkaReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafka, List<KafkaNodePool> nodePools, ClusterCa clusterCa, ClientsCa clientsCa, KafkaMetadataStateManager stateManager) {
            super(reconciliation, kafka, nodePools, createKafkaCluster(reconciliation, kafka, stateManager.getMetadataConfigurationState()), clusterCa, clientsCa, config, supplier, pfa, vertx, stateManager);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return modelWarnings(kafkaStatus)
                    .compose(i -> clusterId(kafkaStatus))
                    .compose(i -> updateKafkaVersion(kafkaStatus))
                    .compose(i -> updateKafkaMetadataMigrationState())
                    .compose(i -> updateKafkaMetadataState(kafkaStatus))
                    .recover(error -> {
                        LOGGER.errorCr(RECONCILIATION, "Reconciliation failed", error);
                        return Future.failedFuture(error);
                    });
        }
    }
}
