/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
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
import io.strimzi.test.mockkube2.MockKube2;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KRaftMigrationMockTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final int REPLICAS = 3;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);

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

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        mockKube.stop();
    }

    private Future<Void> initialize(Kafka initialKafka, KafkaNodePool initialKafkaNodePoolBrokers) {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaCrd()
                .withInitialKafkas(initialKafka)
                .withKafkaNodePoolCrd()
                .withInitialKafkaNodePools(initialKafkaNodePoolBrokers)
                .withStrimziPodSetCrd()
                .withPodController()
                .withServiceController()
                .withDeploymentController()
                .build();
        mockKube.start();

        supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client), ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(), ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), PFA, 2_000);

        podSetController = new StrimziPodSetController(NAMESPACE, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        operator = new KafkaAssemblyOperator(vertx, PFA, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

        return operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME));
    }

    @Test
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    public void testFullMigration(VertxTestContext context) {
        Kafka kafkaInZooKeeperWithMigrationAnno = kafkaWithStateAndAnno(KAFKA, KafkaMetadataState.ZooKeeper, "migration");
        Kafka kafkaInMigrationWithMigrationAnno = kafkaWithStateAndAnno(kafkaInZooKeeperWithMigrationAnno, KafkaMetadataState.KRaftMigration, "migration");
        Kafka kafkaInDualWritingWithMigrationAnno = kafkaWithStateAndAnno(kafkaInMigrationWithMigrationAnno, KafkaMetadataState.KRaftDualWriting, "migration");
        Kafka kafkaInPostMigrationWithEnabledAnno = kafkaWithStateAndAnno(kafkaInDualWritingWithMigrationAnno, KafkaMetadataState.KRaftPostMigration, "enabled");
        Kafka kafkaInPreKraftWithEnabledAnno = kafkaWithStateAndAnno(kafkaInPostMigrationWithEnabledAnno, KafkaMetadataState.PreKRaft, "enabled");

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, creation of the ZooKeeper-based cluster with brokers node pool
        initialize(KAFKA, BROKERS)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // assert metadata is ZooKeeper-based
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.ZooKeeper.name()));
                    // deploying the controllers node pool
                    Crds.kafkaNodePoolOperation(client).inNamespace(NAMESPACE).resource(CONTROLLERS).create();
                })))
                // 2nd reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInZooKeeperWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftMigration ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftMigration.name()));
                    // ... and controllers deployed
                    StrimziPodSet controllersSps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-controllers").get();
                    assertThat(controllersSps, is(notNullValue()));
                    // assert controllers are configured to connect to ZooKeeper and with migration enabled
                    List<ConfigMap> controllersConfigMaps = client.configMaps().inNamespace(NAMESPACE).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-controller")).toList();
                    for (ConfigMap controllerConfigMap : controllersConfigMaps) {
                        String controllerConfig = controllerConfigMap.getData().get("server.config");
                        assertThat(controllerConfig, containsString("process.roles=controller"));
                        assertThat(controllerConfig, containsString("zookeeper.metadata.migration.enable=true"));
                        assertThat(controllerConfig, containsString("zookeeper.connect"));
                    }
                })))
                // 3rd reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInMigrationWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert we are still in KRaftMigration ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftMigration.name()));
                })))
                // 4th reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInMigrationWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftDualWriting ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftDualWriting.name()));
                    // ... and brokers still connected to ZooKeeper, but configured with migration enabled and controllers connection
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(NAMESPACE).list().getItems()
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
                // 5th reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation and in dual-writing
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInDualWritingWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftPostMigration ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftPostMigration.name()));
                    // ... and brokers not connected to ZooKeeper anymore and no migration enabled
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(NAMESPACE).list().getItems()
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
                // 6th reconcile, Kafka custom resource with the strimzi.io/kraft: enabled annotation and in post-migration
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInPostMigrationWithEnabledAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to PreKRaft ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.PreKRaft.name()));
                    // ... and controllers not connected to ZooKeeper anymore and no migration enabled
                    List<ConfigMap> controllersConfigMaps = client.configMaps().inNamespace(NAMESPACE).list().getItems()
                            .stream().filter(cm -> cm.getMetadata().getName().startsWith(CLUSTER_NAME + "-controller")).toList();
                    for (ConfigMap controllerConfigMap : controllersConfigMaps) {
                        String controllerConfig = controllerConfigMap.getData().get("server.config");
                        assertThat(controllerConfig, containsString("process.roles=controller"));
                        assertThat(controllerConfig, not(containsString("zookeeper.metadata.migration.enable=true")));
                        assertThat(controllerConfig, not(containsString("zookeeper.connect")));
                    }
                })))
                // 7th reconcile, Kafka custom resource with the strimzi.io/kraft: enabled annotation and in pre-kraft
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInPreKraftWithEnabledAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaft ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaft.name()));
                    // ... and ZooKeeper not running anymore
                    StrimziPodSet zookeeperSps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-zookeeper").get();
                    assertThat(zookeeperSps, is(nullValue()));
                    reconciliation.flag();
                })));
    }

    @Test
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    public void testRollbackMigration(VertxTestContext context) {
        Kafka kafkaInZooKeeperWithMigrationAnno = kafkaWithStateAndAnno(KAFKA, KafkaMetadataState.ZooKeeper, "migration");
        Kafka kafkaInMigrationWithMigrationAnno = kafkaWithStateAndAnno(kafkaInZooKeeperWithMigrationAnno, KafkaMetadataState.KRaftMigration, "migration");
        Kafka kafkaInDualWritingWithMigrationAnno = kafkaWithStateAndAnno(kafkaInMigrationWithMigrationAnno, KafkaMetadataState.KRaftDualWriting, "migration");
        Kafka kafkaInPostMigrationWithRollbackAnno = kafkaWithStateAndAnno(kafkaInDualWritingWithMigrationAnno, KafkaMetadataState.KRaftPostMigration, "rollback");
        Kafka kafkaInDualWritingWithDisabledAnno = kafkaWithStateAndAnno(kafkaInPostMigrationWithRollbackAnno, KafkaMetadataState.KRaftDualWriting, "disabled");

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, creation of the ZooKeeper-based cluster with brokers node pool
        initialize(KAFKA, BROKERS)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // assert metadata is ZooKeeper-based
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.ZooKeeper.name()));
                    // deploying the controllers node pool
                    Crds.kafkaNodePoolOperation(client).inNamespace(NAMESPACE).resource(CONTROLLERS).create();
                })))
                // 2nd reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInZooKeeperWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftMigration
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftMigration.name()));
                })))
                // 3rd reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInMigrationWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert we are still in KRaftMigration ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftMigration.name()));
                })))
                // 4th reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation and in migration
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInMigrationWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftDualWriting
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftDualWriting.name()));
                })))
                // 5th reconcile, Kafka custom resource with the strimzi.io/kraft: migration annotation and in dual-writing
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInDualWritingWithMigrationAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to KRaftPostMigration
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftPostMigration.name()));
                })))
                // 6th reconcile, Kafka custom resource with the strimzi.io/kraft: rollback annotation and in post-migration
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInPostMigrationWithRollbackAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition back to KRaftDualWriting ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.KRaftDualWriting.name()));
                    // ... and brokers connected to ZooKeeper with migration enabled again
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(NAMESPACE).list().getItems()
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
                    Crds.kafkaNodePoolOperation(client).inNamespace(NAMESPACE).resource(CONTROLLERS).delete();
                })))
                // 7th reconcile, Kafka custom resource with the strimzi.io/kraft: disabled annotation and in dual-writing
                .recover(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInDualWritingWithDisabledAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // verify controllers not running anymore
                    StrimziPodSet controllerSps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-controller").get();
                    assertThat(controllerSps, is(nullValue()));
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafkaInDualWritingWithDisabledAnno))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    // assert transition to ZooKeeper ...
                    assertThat(status.getKafkaMetadataState(), is(KafkaMetadataState.ZooKeeper.name()));
                    // ... and brokers connected to ZooKeeper with migration disabled and no controllers connection
                    List<ConfigMap> brokersConfigMaps = client.configMaps().inNamespace(NAMESPACE).list().getItems()
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

    private Kafka kafkaWithStateAndAnno(Kafka current, KafkaMetadataState metadataState, String kraftAnno) {
        return new KafkaBuilder(current)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, kraftAnno)
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(metadataState.name())
                .endStatus()
                .build();
    }
}
