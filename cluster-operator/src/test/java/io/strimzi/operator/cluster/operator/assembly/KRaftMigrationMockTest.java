/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class KRaftMigrationMockTest {
    private static final String CLUSTER_NAME = "my-cluster";
    private static final int REPLICAS = 3;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);

    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
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
                .withNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                    .withNewUserOperator()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build();

    private final static KafkaNodePool CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
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

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;
    private String namespace;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaNodePoolCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withStrimziPodSetCrd()
                .withPodController()
                .withDeletionController()
                .withServiceController()
                .withDeploymentController()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo)   {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    private Future<Void> initialize() {
        supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client), ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(), ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), PFA, 2_000);

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        operator = new KafkaAssemblyOperator(vertx, PFA, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

        return operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME));
    }

    @Test
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    public void testFullMigration(VertxTestContext context) {
        Kafka initialKafka = new KafkaBuilder(KAFKA)
                .build();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();
        KafkaNodePool initialKafkaNodePoolBrokers = new KafkaNodePoolBuilder(BROKERS)
                .build();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(initialKafkaNodePoolBrokers).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, creation of the ZooKeeper-based cluster with brokers node pool
        initialize()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // assert metadata is ZooKeeper-based
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.ZooKeeper));
                    // deploying the controllers node pool
                    Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(CONTROLLERS).create();
                })))
                // 2nd reconcile, Kafka custom resource updated with the strimzi.io/kraft: migration annotation
                .compose(i -> {
                    Kafka kafkaWithMigrationAnno = kafkaWithKRaftAnno(
                            Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get(), "migration"
                    );
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaWithMigrationAnno).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert transition to KRaftMigration ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftMigration);
                    // ... and controllers deployed
                    StrimziPodSet controllersSps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controllers").get();
                    assertThat(controllersSps, is(notNullValue()));
                    // assert controllers are configured to connect to ZooKeeper and with migration enabled
                    List<ConfigMap> controllersConfigMaps = client.configMaps().inNamespace(namespace).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-controller")).toList();
                    for (ConfigMap controllerConfigMap : controllersConfigMaps) {
                        String controllerConfig = controllerConfigMap.getData().get("server.config");
                        assertThat(controllerConfig, containsString("process.roles=controller"));
                        assertThat(controllerConfig, containsString("zookeeper.metadata.migration.enable=true"));
                        assertThat(controllerConfig, containsString("zookeeper.connect"));
                    }
                })))
                // 3rd reconcile, Kafka custom resource keeps the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert we are still in KRaftMigration ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftMigration);
                })))
                // 4th reconcile, Kafka custom resource keeps the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert transition to KRaftDualWriting ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftDualWriting);
                    // ... and brokers rolled, still connected to ZooKeeper, but configured with migration enabled and controllers connection
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(namespace).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-broker")).toList();
                    for (ConfigMap brokerConfigMap : brokersConfigMaps) {
                        String brokerConfig = brokerConfigMap.getData().get("server.config");
                        assertThat(brokerConfig, not(containsString("process.roles=broker")));
                        assertThat(brokerConfig, containsString("zookeeper.metadata.migration.enable=true"));
                        assertThat(brokerConfig, containsString("zookeeper.connect"));
                        assertThat(brokerConfig, containsString("controller.listener.names"));
                        assertThat(brokerConfig, containsString("controller.quorum.voters"));
                    }
                })))
                // 5th reconcile, Kafka custom resource keeps the strimzi.io/kraft: migration annotation and in dual-writing
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert transition to KRaftPostMigration ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftPostMigration);
                    // ... and brokers rolled, not connected to ZooKeeper anymore and no migration enabled
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(namespace).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-broker")).toList();
                    for (ConfigMap brokerConfigMap : brokersConfigMaps) {
                        String brokerConfig = brokerConfigMap.getData().get("server.config");
                        assertThat(brokerConfig, containsString("process.roles=broker"));
                        assertThat(brokerConfig, not(containsString("zookeeper.metadata.migration.enable=true")));
                        assertThat(brokerConfig, not(containsString("zookeeper.connect")));
                        assertThat(brokerConfig, containsString("controller.listener.names"));
                        assertThat(brokerConfig, containsString("controller.quorum.voters"));
                    }
                })))
                // 6th reconcile, Kafka custom resource updated with the strimzi.io/kraft: enabled annotation and in post-migration
                .compose(i -> {
                    Kafka kafkaWithEnabledAnno = kafkaWithKRaftAnno(
                            Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get(), "enabled"
                    );
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaWithEnabledAnno).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to PreKRaft ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.PreKRaft);
                    // ... and controllers rolled, not connected to ZooKeeper anymore and no migration enabled
                    List<ConfigMap> controllersConfigMaps = client.configMaps().inNamespace(namespace).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-controller")).toList();
                    for (ConfigMap controllerConfigMap : controllersConfigMaps) {
                        String controllerConfig = controllerConfigMap.getData().get("server.config");
                        assertThat(controllerConfig, containsString("process.roles=controller"));
                        assertThat(controllerConfig, not(containsString("zookeeper.metadata.migration.enable=true")));
                        assertThat(controllerConfig, not(containsString("zookeeper.connect")));
                    }
                })))
                // 7th reconcile, Kafka custom resource keeps the strimzi.io/kraft: enabled annotation and in pre-kraft
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaft ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaft);
                    // ... and ZooKeeper not running anymore
                    StrimziPodSet zookeeperSps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-zookeeper").get();
                    assertThat(zookeeperSps, is(nullValue()));
                    reconciliation.flag();
                })));
    }

    @Test
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    public void testRollbackMigration(VertxTestContext context) {
        Kafka initialKafka = new KafkaBuilder(KAFKA)
                .build();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();
        KafkaNodePool initialKafkaNodePoolBrokers = new KafkaNodePoolBuilder(BROKERS)
                .build();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(initialKafkaNodePoolBrokers).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, creation of the ZooKeeper-based cluster with brokers node pool
        initialize()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // assert metadata is ZooKeeper-based
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.ZooKeeper));
                    // deploying the controllers node pool
                    Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(CONTROLLERS).create();
                })))
                // 2nd reconcile, Kafka custom resource updated with the strimzi.io/kraft: migration annotation
                .compose(i -> {
                    Kafka kafkaWithMigrationAnno = kafkaWithKRaftAnno(
                            Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get(), "migration"
                    );
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaWithMigrationAnno).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert transition to KRaftMigration
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftMigration);
                })))
                // 3rd reconcile, Kafka custom resource keeps the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert we are still in KRaftMigration ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftMigration);
                })))
                // 4th reconcile, Kafka custom resource keeps the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftDualWriting
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftDualWriting);
                })))
                // 5th reconcile, Kafka custom resource keeps the strimzi.io/kraft: migration annotation and in dual-writing
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert transition to KRaftPostMigration
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftPostMigration);
                })))
                // 6th reconcile, Kafka custom resource updated with the strimzi.io/kraft: rollback annotation and in post-migration
                .compose(i -> {
                    Kafka kafkaWithRollbackAnno = kafkaWithKRaftAnno(
                            Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get(), "rollback"
                    );
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaWithRollbackAnno).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    // assert transition back to KRaftDualWriting ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.KRaftDualWriting);
                    // ... and brokers rolled and connected to ZooKeeper with migration enabled again
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(namespace).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-broker")).toList();
                    for (ConfigMap brokerConfigMap : brokersConfigMaps) {
                        String brokerConfig = brokerConfigMap.getData().get("server.config");
                        assertThat(brokerConfig, not(containsString("process.roles=broker")));
                        assertThat(brokerConfig, containsString("zookeeper.metadata.migration.enable=true"));
                        assertThat(brokerConfig, containsString("zookeeper.connect"));
                        assertThat(brokerConfig, containsString("controller.listener.names"));
                        assertThat(brokerConfig, containsString("controller.quorum.voters"));
                    }
                    // delete the controllers node pool
                    Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(CONTROLLERS).delete();
                })))
                // 7th reconcile, Kafka custom resource updated with the strimzi.io/kraft: disabled annotation and in dual-writing
                .compose(i -> {
                    Kafka kafkaWithDisabledAnno = kafkaWithKRaftAnno(
                            Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get(), "disabled"
                    );
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaWithDisabledAnno).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // verify controllers not running anymore
                    StrimziPodSet controllerSps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controller").get();
                    assertThat(controllerSps, is(nullValue()));
                })))
                // 8th reconcile, Kafka custom resource keeps the strimzi.io/kraft: disabled annotation and in dual-writing
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to ZooKeeper ...
                    assertMetadataStateInKafkaStatus(KafkaMetadataState.ZooKeeper);
                    // ... and brokers rolled, connected to ZooKeeper with migration disabled and no controllers connection
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(namespace).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-broker")).toList();
                    for (ConfigMap brokerConfigMap : brokersConfigMaps) {
                        String brokerConfig = brokerConfigMap.getData().get("server.config");
                        assertThat(brokerConfig, not(containsString("process.roles=broker")));
                        assertThat(brokerConfig, not(containsString("zookeeper.metadata.migration.enable=true")));
                        assertThat(brokerConfig, containsString("zookeeper.connect"));
                        assertThat(brokerConfig, not(containsString("controller.listener.names")));
                        assertThat(brokerConfig, not(containsString("controller.quorum.voters")));
                    }
                    reconciliation.flag();
                })));
    }

    private Kafka kafkaWithKRaftAnno(Kafka current, String kraftAnno) {
        return new KafkaBuilder(current)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, kraftAnno)
                .endMetadata()
                .build();
    }

    private void assertMetadataStateInKafkaStatus(KafkaMetadataState metadataState) {
        KafkaStatus status = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get().getStatus();
        assertThat(status.getKafkaMetadataState(), is(metadataState));
    }
}
