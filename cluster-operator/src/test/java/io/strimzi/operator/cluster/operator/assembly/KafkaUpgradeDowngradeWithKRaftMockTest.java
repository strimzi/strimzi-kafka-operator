/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
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
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KafkaUpgradeDowngradeWithKRaftMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUpgradeDowngradeWithKRaftMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withAnnotations(Map.of(
                            Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled",
                            Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"
                    ))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
    private static final KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("mixed")
                    .withNamespace(NAMESPACE)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withGeneration(1L)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"))).build())
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

    private AtomicInteger metadataLevel;

    /*
     * HELPER METHODS
     */

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

    private Future<Void> initialize(Kafka initialKafka, String initialMetadataVersion)   {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaNodePoolCrd()
                .withInitialKafkaNodePools(POOL_MIXED)
                .withKafkaCrd()
                .withInitialKafkas(initialKafka)
                .withStrimziPodSetCrd()
                .withPodController()
                .withServiceController()
                .withDeploymentController()
                .build();
        mockKube.start();

        Admin mockAdmin = ResourceUtils.adminClient();
        metadataLevel = new AtomicInteger(metadataVersionToLevel(initialMetadataVersion));
        mockAdminClient(mockAdmin);
        supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(mockAdmin), ResourceUtils.zookeeperScalerProvider(), ResourceUtils.metricsProvider(), PFA, 2_000);

        podSetController = new StrimziPodSetController(NAMESPACE, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), VERSIONS)
                .with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "10000")
                .with(ClusterOperatorConfig.FEATURE_GATES.key(), "+UseKRaft")
                .build();

        operator = new KafkaAssemblyOperator(vertx, PFA, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME));
    }

    private void mockAdminClient(Admin mockAdminClient)  {
        // Mock getting the current metadata version
        FinalizedVersionRange fvr = mock(FinalizedVersionRange.class);
        when(fvr.maxVersionLevel()).thenReturn((short) metadataLevel.get());
        FeatureMetadata fm = mock(FeatureMetadata.class);
        when(fm.finalizedFeatures()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, fvr));
        DescribeFeaturesResult dfr = mock(DescribeFeaturesResult.class);
        when(dfr.featureMetadata()).thenReturn(KafkaFuture.completedFuture(fm));
        when(mockAdminClient.describeFeatures()).thenReturn(dfr);

        // Mock updating metadata version
        UpdateFeaturesResult ufr = mock(UpdateFeaturesResult.class);
        when(ufr.values()).thenReturn(Map.of(KRaftMetadataManager.METADATA_VERSION_KEY, KafkaFuture.completedFuture(null)));
        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<Map<String, FeatureUpdate>> updateCaptor = ArgumentCaptor.forClass(Map.class);
        when(mockAdminClient.updateFeatures(updateCaptor.capture(), any())).thenAnswer(i -> {
            Map<String, FeatureUpdate> update = i.getArgument(0);
            metadataLevel.set(update.get(KRaftMetadataManager.METADATA_VERSION_KEY).maxVersionLevel());

            return ufr;
        });
    }

    private Kafka kafkaWithVersions(String kafkaVersion, String desiredMetadataVersion, String currentMetadataVersion)   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(kafkaVersion)
                    .endKafka()
                .endSpec()
                .build();

        if (desiredMetadataVersion != null) {
            kafka.getSpec().getKafka().setMetadataVersion(desiredMetadataVersion);
        }

        if (currentMetadataVersion != null) {
            kafka.setStatus(new KafkaStatusBuilder()
                        .withKafkaVersion("old")
                        .withOperatorLastSuccessfulVersion("old")
                        .withKafkaMetadataVersion(currentMetadataVersion)
                    .build());
        }

        return kafka;
    }

    private Kafka kafkaWithVersions(String kafkaVersion)   {
        return new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(kafkaVersion)
                    .endKafka()
                .endSpec()
                .withNewStatus()
                    .withOperatorLastSuccessfulVersion("old")
                    .withKafkaVersion(kafkaVersion)
                .endStatus()
                .build();
    }

    /**
     * Converts String format of metadata version to the short format
     *
     * @param metadataVersion   String metadata version
     *
     * @return  Short based metadata level
     */
    private short metadataVersionToLevel(String metadataVersion)    {
        return MetadataVersion.fromVersionString(metadataVersion).featureLevel();
    }

    /**
     * Converts the short format of metadata version to the string format
     *
     * @param metadataLevel     Short based metadata level
     *
     * @return  String based version
     */
    private String metadataLevelToVersion(short metadataLevel)    {
        return MetadataVersion.fromFeatureLevel(metadataLevel).version();
    }

    private void assertMetadataVersion(String metadataVersion) {
        assertThat(metadataLevelToVersion((short) metadataLevel.get()), is(metadataVersion));
    }

    private void assertVersionsInKafkaStatus(KafkaStatus status, String operatorVersion, String kafkaVersion, String metadataVersion) {
        assertThat(status.getOperatorLastSuccessfulVersion(), is(operatorVersion));
        assertThat(status.getKafkaVersion(), is(kafkaVersion));
        assertThat(status.getKafkaMetadataVersion(), is(metadataVersion));
    }

    private void assertVersionsInStrimziPodSet(String kafkaVersion, String image)  {
        StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-mixed").get();
        assertThat(sps.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));

        sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(image));
        });

        for (int i = 0; i < 3; i++) {
            Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-mixed-" + i).get();
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(image));
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));
        }
    }

    /*
     * UPGRADE TESTS
     */

    // Checks the upgrade without the metadata version being set. Runs 3 reconciliation:
    //   - First to create the initial Kafka cluster
    //   - Second to upgrade the Kafka version
    //   - Third to upgrade the metadata version
    @Test
    public void testUpgradeWithoutMetadataVersion(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, null, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(initialKafka, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION);

                    reconciliation.flag();
                })));
    }

    // Checks the upgrade with the metadata version being updated to the same version as Kafka version. Runs 3 reconciliation:
    //   - First to create the initial Kafka cluster
    //   - Second to upgrade the Kafka version
    //   - Third to upgrade the metadata version
    @Test
    public void testUpgradeWithNewMessageAndProtocolVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION,
                null);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_METADATA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(initialKafka, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION);

                    reconciliation.flag();
                })));
    }

    // Checks the upgrade with the metadata version being set after Kafka version upgrade. Runs 3 reconciliation:
    //   - First to create the initial Kafka cluster
    //   - Second to upgrade the Kafka version
    //   - Third to upgrade the metadata version
    @Test
    public void testUpgradeWithNewMessageAndProtocolVersionsInSeparatePhases(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION,
                null);

        Kafka updatedKafka1 = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Kafka updatedKafka2 = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_METADATA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(initialKafka, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka1))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka2))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION);

                    reconciliation.flag();
                })));
    }

    // Checks the upgrade with no version set in the Kafka CR. Runs 3 reconciliation:
    //   - First to create the initial Kafka cluster
    //   - Second to upgrade the Kafka version
    //   - Third to upgrade the metadata version
    @Test
    public void testUpgradeWithoutAnyVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION); // We have to use the version for the initial cluster

        Kafka updatedKafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withKafkaMetadataVersion(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)
                .endStatus()
                .build(); // No version used here

        Checkpoint reconciliation = context.checkpoint();
        initialize(initialKafka, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get()
                            .getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION);

                    reconciliation.flag();
                })));
    }

    /*
     * DOWNGRADE TESTS
     */

    // Test regular downgrade with metadata versions defined everywhere and properly rolled out to all brokers.
    // The metadata versions used is the same as Kafka version we downgrade to.
    @Test
    public void testDowngradeWhenOldMetadataVersionIsUsed(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION,
                null);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(initialKafka, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(status -> context.verify(() -> {
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION);

                    reconciliation.flag();
                })));
    }

    // Tests that downgrade fails when the metadata version does not allow downgrading
    @Test
    public void testDowngradeFailingWhenNewMetadataVersionIsUsed(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_METADATA_VERSION,
                null);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION,
                KafkaVersionTestUtils.LATEST_METADATA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(initialKafka, KafkaVersionTestUtils.LATEST_METADATA_VERSION)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v, is(instanceOf(ReconciliationException.class)));
                    assertThat(v.getCause(), is(instanceOf(KafkaUpgradeException.class)));
                    assertThat(v.getCause().getMessage(), is("The current metadata version (" + KafkaVersionTestUtils.LATEST_METADATA_VERSION + ") has to be lower or equal to the Kafka broker version we are downgrading to (" + KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + ")"));

                    KafkaStatus status = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
                    assertVersionsInKafkaStatus(status, KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_METADATA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    assertMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION);

                    reconciliation.flag();
                })));
    }
}
