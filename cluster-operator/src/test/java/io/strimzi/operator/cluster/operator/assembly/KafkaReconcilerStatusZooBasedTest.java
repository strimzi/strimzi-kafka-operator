/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerStatusZooBasedTest {
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
    private static WorkerExecutor sharedWorkerExecutor;

    @BeforeAll
    public static void beforeAll()  {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll()    {
        sharedWorkerExecutor.close();
        vertx.close();
    }

    @Test
    public void testKafkaReconcilerStatus(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withReplicas(1)
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check ClusterID
            assertThat(status.getClusterId(), is("CLUSTERID"));

            // Check kafka version
            assertThat(status.getKafkaVersion(), is(VERSIONS.defaultVersion().version()));

            // Check model warning conditions
            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("Warning"));
            assertThat(status.getConditions().get(0).getReason(), is("KafkaStorage"));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithSpecCheckerWarnings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                KAFKA
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check model warning conditions
            assertThat(status.getConditions().size(), is(2));
            assertThat(status.getConditions().get(0).getType(), is("Warning"));
            assertThat(status.getConditions().get(0).getReason(), is("KafkaDefaultReplicationFactor"));
            assertThat(status.getConditions().get(1).getType(), is("Warning"));
            assertThat(status.getConditions().get(1).getReason(), is("KafkaMinInsyncReplicas"));

            async.flag();
        }));
    }

    static class MockKafkaReconcilerStatusTasks extends KafkaReconciler {
        private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MockKafkaReconcilerStatusTasks.class.getName());

        public MockKafkaReconcilerStatusTasks(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr) {
            super(reconciliation, kafkaCr, null, createKafkaCluster(reconciliation, supplier, kafkaCr), CLUSTER_CA, CLIENTS_CA, CO_CONFIG, supplier, PFA, vertx, new KafkaMetadataStateManager(reconciliation, kafkaCr));
        }

        private static KafkaCluster createKafkaCluster(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr)   {
            return  KafkaClusterCreator.createKafkaCluster(
                    reconciliation,
                    kafkaCr,
                    null,
                    Map.of(),
                    Map.of(),
                    KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE,
                    new KafkaMetadataStateManager(reconciliation, kafkaCr).getMetadataConfigurationState(),
                    VERSIONS,
                    supplier.sharedEnvironmentProvider);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return modelWarnings(kafkaStatus)
                    .compose(i -> initClientAuthenticationCertificates())
                    .compose(i -> listeners())
                    .compose(i -> clusterId(kafkaStatus))
                    .compose(i -> nodePortExternalListenerStatus())
                    .compose(i -> addListenersToKafkaStatus(kafkaStatus))
                    .compose(i -> updateKafkaVersion(kafkaStatus))
                    .recover(error -> {
                        LOGGER.errorCr(reconciliation, "Reconciliation failed", error);
                        return Future.failedFuture(error);
                    });
        }

        @Override
        protected Future<Void> listeners()  {
            listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
            listenerReconciliationResults.bootstrapNodePorts.put("external-9094", 31234);
            listenerReconciliationResults.listenerStatuses.add(new ListenerStatusBuilder().withName("external").build());

            return Future.succeededFuture();
        }

        @Override
        protected Future<Void> initClientAuthenticationCertificates() {
            coTlsPemIdentity = new TlsPemIdentity(null, null);
            return Future.succeededFuture();
        }
    }
}
