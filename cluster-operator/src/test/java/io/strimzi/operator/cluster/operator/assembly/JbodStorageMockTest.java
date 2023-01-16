/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube2.MockKube2;
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class JbodStorageMockTest {

    private static final String NAMESPACE = "test-jbod-storage";
    private static final String NAME = "my-kafka";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private Kafka kafka;
    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;
    private KafkaAssemblyOperator operator;
    private StrimziPodSetController podSetController;

    private List<SingleVolumeStorage> volumes;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
    }

    @BeforeEach
    public void init() {
        this.volumes = new ArrayList<>(2);

        volumes.add(new PersistentClaimStorageBuilder()
                .withId(0)
                .withDeleteClaim(true)
                .withSize("100Gi").build());
        volumes.add(new PersistentClaimStorageBuilder()
                .withId(1)
                .withDeleteClaim(false)
                .withSize("100Gi").build());

        this.kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewJbodStorage()
                            .withVolumes(volumes)
                        .endJbodStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                    .endZookeeper()
                .endSpec()
                .build();

        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaCrd()
                .withInitialKafkas(kafka)
                .withStrimziPodSetCrd()
                .withDeploymentController()
                .withPodController()
                .withStatefulSetController()
                .withServiceController()
                .build();
        mockKube.start();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        // creating the Kafka operator
        ResourceOperatorSupplier ros =
                new ResourceOperatorSupplier(JbodStorageMockTest.vertx, this.client,
                        ResourceUtils.zookeeperLeaderFinder(JbodStorageMockTest.vertx, this.client),
                        ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(),
                        ResourceUtils.metricsProvider(), pfa, 60_000L);

        podSetController = new StrimziPodSetController(NAMESPACE, Labels.EMPTY, ros.kafkaOperator, ros.strimziPodSetOperator, ros.podOperations, ros.metricsProvider, ClusterOperatorConfig.DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
        podSetController.start();

        this.operator = new KafkaAssemblyOperator(JbodStorageMockTest.vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), ros,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS, 2_000));
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        mockKube.stop();
    }

    @Test
    public void testJbodStorageCreatesPersistentVolumeClaimsMatchingKafkaVolumes(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);

                for (int i = 0; i < this.kafka.getSpec().getKafka().getReplicas(); i++) {
                    for (SingleVolumeStorage volume : this.volumes) {
                        if (volume instanceof PersistentClaimStorage) {

                            String expectedPvcName = VolumeUtils.createVolumePrefix(volume.getId(), true) + "-" + KafkaResources.kafkaPodName(NAME, i);
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

        Kafka kafkaWithNewJbodVolume = new KafkaBuilder(kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(kafka);
        Set<String> expectedPvcsWithNewJbodStorageVolume = expectedPvcs(kafkaWithNewJbodVolume);

        // reconcile for kafka cluster creation
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcs));
            })))
            .compose(v -> {
                Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(NAME).patch(kafkaWithNewJbodVolume);
                // reconcile kafka cluster with new Jbod storage
                return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME));
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
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

        Kafka kafkaWithRemovedJbodVolume = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(kafka);
        Set<String> expectedPvcsWithRemovedJbodStorageVolume = expectedPvcs(kafkaWithRemovedJbodVolume);

        // reconcile for kafka cluster creation
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcs));
            })))
            .compose(v -> {
                Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(NAME).patch(kafkaWithRemovedJbodVolume);
                // reconcile kafka cluster with a Jbod storage volume removed
                return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME));
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
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

        Kafka kafkaWithUpdatedJbodVolume = new KafkaBuilder(this.kafka)
                .editSpec()
                    .editKafka()
                        .withStorage(new JbodStorageBuilder().withVolumes(volumes).build())
                    .endKafka()
                .endSpec()
                .build();

        Set<String> expectedPvcs = expectedPvcs(kafka);
        Set<String> expectedPvcsWithUpdatedJbodStorageVolume = expectedPvcs(kafkaWithUpdatedJbodVolume);


        // reconcile for kafka cluster creation
        operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcs));
            })))
            .compose(v -> {
                Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(NAME).patch(kafkaWithUpdatedJbodVolume);
                // reconcile kafka cluster with a Jbod storage volume removed
                return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME));
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = getPvcs(NAMESPACE, NAME);
                Set<String> pvcsNames = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet());
                assertThat(pvcsNames, is(expectedPvcsWithUpdatedJbodStorageVolume));
                async.flag();
            })));
    }

    private Set<String> expectedPvcs(Kafka kafka) {
        Set<String> expectedPvcs = new HashSet<>();
        for (int i = 0; i < kafka.getSpec().getKafka().getReplicas(); i++) {
            for (SingleVolumeStorage volume : ((JbodStorage) kafka.getSpec().getKafka().getStorage()).getVolumes()) {
                if (volume instanceof PersistentClaimStorage) {
                    expectedPvcs.add(AbstractModel.VOLUME_NAME + "-" + volume.getId() + "-"
                            + KafkaResources.kafkaPodName(NAME, i));
                }
            }
        }
        return expectedPvcs;
    }

    private List<PersistentVolumeClaim> getPvcs(String namespace, String name) {
        String kafkaStsName = KafkaResources.kafkaStatefulSetName(name);
        Labels pvcSelector = Labels.forStrimziCluster(name).withStrimziKind(Kafka.RESOURCE_KIND).withStrimziName(kafkaStsName);
        return client.persistentVolumeClaims()
                .inNamespace(namespace)
                .withLabels(pvcSelector.toMap())
                .list().getItems();
    }
}
