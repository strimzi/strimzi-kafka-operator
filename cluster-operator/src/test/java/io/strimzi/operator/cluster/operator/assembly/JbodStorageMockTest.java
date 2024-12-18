/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class JbodStorageMockTest {
    private static final String NAME = "my-cluster";
    private static final String NODE_POOL_NAME = "mixed";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace = "test-jbod-storage";
    private KafkaNodePool kafkaNodePool;
    private KafkaAssemblyOperator operator;
    private StrimziPodSetController podSetController;

    private List<SingleVolumeStorage> volumes;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaNodePoolCrd()
                .withKafkaCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withStrimziPodSetCrd()
                .withDeploymentController()
                .withPodController()
                .withServiceController()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
        sharedWorkerExecutor.close();
        vertx.close();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        this.volumes = new ArrayList<>(2);

        volumes.add(new PersistentClaimStorageBuilder()
                .withId(0)
                .withDeleteClaim(true)
                .withSize("100Gi").build());
        volumes.add(new PersistentClaimStorageBuilder()
                .withId(1)
                .withDeleteClaim(false)
                .withSize("100Gi").build());

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(NAME)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
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
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).create();

        this.kafkaNodePool = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(NODE_POOL_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME))
                    .withGeneration(1L)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(volumes)
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .endSpec()
                .build();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(kafkaNodePool).create();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        // creating the Kafka operator
        ResourceOperatorSupplier ros =
                new ResourceOperatorSupplier(vertx, client, ResourceUtils.adminClientProvider(),
                        ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), pfa);

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, ros.kafkaOperator, ros.connectOperator, ros.mirrorMaker2Operator, ros.strimziPodSetOperator, ros.podOperations, ros.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        this.operator = new KafkaAssemblyOperator(JbodStorageMockTest.vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), ros,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
    }

    @Test
    public void testJbodStorageCreatesPVCsMatchingKafkaVolumes(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();

                for (int i = 0; i < this.kafkaNodePool.getSpec().getReplicas(); i++) {
                    for (SingleVolumeStorage volume : this.volumes) {
                        if (volume instanceof PersistentClaimStorage) {
                            String expectedPvcName = VolumeUtils.createVolumePrefix(volume.getId(), true) + "-" + KafkaResources.kafkaPodName(NAME, NODE_POOL_NAME, i);
                            List<PersistentVolumeClaim> matchingPvcs = pvcs.stream()
                                    .filter(pvc -> pvc.getMetadata().getName().equals(expectedPvcName))
                                    .collect(Collectors.toList());
                            assertThat("Exactly one pvc should have the name " + expectedPvcName + " in :\n" + pvcs,
                                    matchingPvcs, Matchers.hasSize(1));

                            PersistentVolumeClaim pvc = matchingPvcs.get(0);
                            boolean isDeleteClaim = ((PersistentClaimStorage) volume).isDeleteClaim();
                            assertThat("deleteClaim value did not match for volume : " + volume,
                                    Annotations.booleanAnnotation(pvc, Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM,
                                            false),
                                    is(isDeleteClaim));

                        }
                    }
                }

                async.flag();
            })));
    }

    @Test
    public void testReconcileWithNewVolumeAddedToJbodStorage(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        // Add a new volume to Jbod Storage
        volumes.add(new PersistentClaimStorageBuilder()
                .withId(2)
                .withDeleteClaim(false)
                .withSize("100Gi").build());

        KafkaNodePool kafkaNodePoolWithNewJbodVolume = new KafkaNodePoolBuilder(kafkaNodePool)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(kafkaNodePool);
        Set<String> expectedPvcsWithNewJbodStorageVolume = expectedPvcs(kafkaNodePoolWithNewJbodVolume);

        // reconcile for kafka cluster creation
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcs));
            })))
            .compose(v -> {
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName(NODE_POOL_NAME).patch(kafkaNodePoolWithNewJbodVolume);
                // reconcile kafka cluster with new Jbod storage
                return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, NAME));
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcsWithNewJbodStorageVolume));
                async.flag();
            })));


    }

    @Test
    public void testReconcileWithVolumeRemovedFromJbodStorage(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        // remove a volume from the Jbod Storage
        volumes.remove(0);

        KafkaNodePool kafkaNodePoolWithNewJbodVolume = new KafkaNodePoolBuilder(kafkaNodePool)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(kafkaNodePool);
        Set<String> expectedPvcsWithRemovedJbodStorageVolume = expectedPvcs(kafkaNodePoolWithNewJbodVolume);

        // reconcile for kafka cluster creation
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcs));
            })))
            .compose(v -> {
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName(NODE_POOL_NAME).patch(kafkaNodePoolWithNewJbodVolume);
                // reconcile kafka cluster with a Jbod storage volume removed
                return operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, NAME));
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcsWithRemovedJbodStorageVolume));
                async.flag();
            })));
    }

    @Test
    public void testReconcileWithUpdateVolumeIdJbod(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        // trying to update id for a volume from in the JBOD storage
        volumes.get(0).setId(3);

        KafkaNodePool kafkaNodePoolWithNewJbodVolume = new KafkaNodePoolBuilder(kafkaNodePool)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(kafkaNodePool);
        Set<String> expectedPvcsWithUpdatedJbodStorageVolume = expectedPvcs(kafkaNodePoolWithNewJbodVolume);

        // reconcile for kafka cluster creation
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcs));
            })))
            .compose(v -> {
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName(NODE_POOL_NAME).patch(kafkaNodePoolWithNewJbodVolume);
                // reconcile kafka cluster with a Jbod storage volume removed
                return operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, NAME));
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs();
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcsWithUpdatedJbodStorageVolume));
                async.flag();
            })));
    }

    private Set<String> expectedPvcs(KafkaNodePool nodePool) {
        Set<String> expectedPvcs = new HashSet<>();
        for (int i = 0; i < nodePool.getSpec().getReplicas(); i++) {
            for (SingleVolumeStorage volume : ((JbodStorage) nodePool.getSpec().getStorage()).getVolumes()) {
                if (volume instanceof PersistentClaimStorage) {
                    expectedPvcs.add(VolumeUtils.DATA_VOLUME_NAME + "-" + volume.getId() + "-" + KafkaResources.kafkaPodName(NAME, NODE_POOL_NAME, i));
                }
            }
        }
        return expectedPvcs;
    }

    private List<PersistentVolumeClaim> getPvcs() {
        Labels pvcSelector = Labels.forStrimziCluster(NAME).withStrimziKind(Kafka.RESOURCE_KIND).withStrimziName(KafkaResources.kafkaComponentName(NAME));
        return client.persistentVolumeClaims()
                .inNamespace(namespace)
                .withLabels(pvcSelector.toMap())
                .list().getItems();
    }
}
