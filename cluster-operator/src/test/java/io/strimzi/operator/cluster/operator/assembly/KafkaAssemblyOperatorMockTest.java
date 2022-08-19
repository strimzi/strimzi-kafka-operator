/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.StrimziPodSetStatus;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.storage.Storage.deleteClaim;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static Vertx vertx;

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_16;

    private int kafkaReplicas;
    private Storage kafkaStorage;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    /*
     * Params contains the parameterized fields of different configurations of a Kafka and Zookeeper Strimzi cluster
     */

    private Kafka cluster;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    /*
     * init is equivalent to a @BeforeEach method
     * since this is a parameterized set, the tests params are only available at test start
     * This must be called before each test
     */

    @BeforeEach
    public void init() {

        cluster = new KafkaBuilder()
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
                         .withNewPersistentClaimStorage()
                         .withSize("123")
                         .withStorageClass("foo")
                         .withDeleteClaim(true)
                         .endPersistentClaimStorage()
                     .endKafka()
                     .withNewZookeeper()
                         .withReplicas(3)
                        .withNewPersistentClaimStorage()
                         .withSize("123")
                         .withStorageClass("foo")
                         .withDeleteClaim(true)
                         .endPersistentClaimStorage()
                         .endZookeeper()
                 .endSpec()
                 .build();

        // Configure replicas and storage
        this.kafkaReplicas = cluster.getSpec().getKafka().getReplicas();
        this.kafkaStorage = cluster.getSpec().getKafka().getStorage();

        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaCrd()
                .withInitialKafkas(cluster)
                .withStrimziPodSetCrd()
                .withDeploymentController()
                .withPodController()
                .withServiceController()
                .build();
        mockKube.start();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, kubernetesVersion);
        supplier = supplierWithMocks();
        podSetController = new StrimziPodSetController(NAMESPACE, Labels.EMPTY, supplier.kafkaOperator, supplier.strimziPodSetOperator, supplier.podOperations, ClusterOperatorConfig.DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

    }

    private ResourceOperatorSupplier supplierWithMocks() {
        ZookeeperLeaderFinder leaderFinder = ResourceUtils.zookeeperLeaderFinder(vertx, client);
        return new ResourceOperatorSupplier(vertx, client, leaderFinder,
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(false, kubernetesVersion), 2_000);
    }


    private Future<Void> initialReconcile(VertxTestContext context) {
        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                sps.setStatus(new StrimziPodSetStatus());

                assertThat(sps, is(notNullValue()));

                sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                });

                StrimziPodSet zkSps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-zookeeper").get();
                zkSps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                });

                //Removed in StrimziPodSets
                //assertThat(client.configMaps().inNamespace(NAMESPACE).withName(KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.configMaps().inNamespace(NAMESPACE).withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.zookeeperSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
            })));
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        mockKube.stop();
    }

    /** Create a cluster from a Kafka */
    @Test
    public void testReconcile(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeedingThenComplete())
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    @Test
    public void testReconcileReplacesAllDeletedSecrets(VertxTestContext context) {
        initialReconcileThenDeleteSecretsThenReconcile(context,
                KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.kafkaSecretName(CLUSTER_NAME),
                KafkaResources.zookeeperSecretName(CLUSTER_NAME),
                ClusterOperator.secretName(CLUSTER_NAME));
    }

    /**
     * Test the operator re-creates secrets if they get deleted
     */
    private void initialReconcileThenDeleteSecretsThenReconcile(VertxTestContext context, String... secrets) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    client.secrets().inNamespace(NAMESPACE).withName(secret).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    assertThat("Expected secret " + secret + " to not exist",
                            client.secrets().inNamespace(NAMESPACE).withName(secret).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    assertThat("Expected secret " + secret + " to have been recreated",
                            client.secrets().inNamespace(NAMESPACE).withName(secret).get(), is(notNullValue()));
                }
                async.flag();
            })));
    }

    /**
     * Test the operator re-creates services if they get deleted
     */
    private void initialReconcileThenDeleteServicesThenReconcile(VertxTestContext context, String... services) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service : services) {
                    client.services().inNamespace(NAMESPACE).withName(service).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    assertThat("Expected service " + service + " to be not exist",
                            client.services().inNamespace(NAMESPACE).withName(service).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service: services) {
                    assertThat("Expected service " + service + " to have been recreated",
                            client.services().inNamespace(NAMESPACE).withName(service).get(), is(notNullValue()));
                }
                async.flag();
            })));
    }

    @Test
    public void testReconcileReplacesDeletedZookeeperServices(VertxTestContext context) {
        initialReconcileThenDeleteServicesThenReconcile(context,
                KafkaResources.zookeeperServiceName(CLUSTER_NAME),
                KafkaResources.zookeeperHeadlessServiceName(CLUSTER_NAME));
    }

    @Test
    public void testReconcileReplacesDeletedKafkaServices(VertxTestContext context) {
        initialReconcileThenDeleteServicesThenReconcile(context,
                KafkaResources.bootstrapServiceName(CLUSTER_NAME),
                KafkaResources.brokersServiceName(CLUSTER_NAME));
    }

    @Test
    public void testReconcileReplacesDeletedZookeeperPodSet(VertxTestContext context) {
        String podSetName = CLUSTER_NAME + "-zookeeper";
        initialReconcileThenDeletePodSetsThenReconcile(context, podSetName);
    }

    @Test
    public void testReconcileReplacesDeletedKafkaPodSet(VertxTestContext context) {
        String podSetName = CLUSTER_NAME + "-kafka";
        initialReconcileThenDeletePodSetsThenReconcile(context, podSetName);
    }

    private void initialReconcileThenDeletePodSetsThenReconcile(VertxTestContext context, String podSetName) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(podSetName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                assertThat("Expected sts " + podSetName + " should not exist",
                        supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(podSetName).get(), is(nullValue()));

                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("Expected sts " + podSetName + " should have been re-created",
                        supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get(), is(notNullValue()));
                async.flag();
            })));
    }

    @Test
    public void testReconcileUpdatesKafkaPersistentVolumes(VertxTestContext context) {
        assumeTrue(kafkaStorage instanceof PersistentClaimStorage, "Parameterized Test only runs for Params with Kafka Persistent storage");

        String originalStorageClass = Storage.storageClass(kafkaStorage);

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(originalStorageClass, is("foo"));

                // Try to update the storage class
                String changedClass = originalStorageClass + "2";

                Kafka patchedPersistenceKafka = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorage()
                                    .withStorageClass(changedClass)
                                    .withSize("123")
                                .endPersistentClaimStorage()
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), patchedPersistenceKafka);

                LOGGER.info("Updating with changed storage class");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the storage class was not changed
                assertThat(((PersistentClaimStorage) kafkaStorage).getStorageClass(), is(originalStorageClass));
                async.flag();
            })));
    }

    private Resource<Kafka> kafkaAssembly(String namespace, String name) {
        return client.resources(Kafka.class, KafkaList.class)
                .inNamespace(namespace).withName(name);
    }

// Doubt in this method. Particularly stuck on how to get the Volumes and the init container using the PodSet


    @Test
    public void testReconcileUpdatesKafkaStorageType(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Update the storage type
                // ephemeral -> persistent
                // or
                // persistent -> ephemeral
                Kafka updatedStorageKafka = null;
                System.out.println(cluster.getSpec().getKafka().getStorage().getClass());
                if (cluster.getSpec().getKafka().getStorage() instanceof EphemeralStorage) {
                    updatedStorageKafka = new KafkaBuilder(cluster)
                            .editSpec()
                                .editKafka()
                                    .withNewPersistentClaimStorage()
                                        .withSize("123")
                                    .endPersistentClaimStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else if (cluster.getSpec().getKafka().getStorage() instanceof PersistentClaimStorage) {
                    updatedStorageKafka = new KafkaBuilder(cluster)
                            .editSpec()
                                .editKafka()
                                    .withNewEphemeralStorage()
                                    .endEphemeralStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else {
                    context.failNow(new Exception("If storage is not ephemeral or persistent something has gone wrong"));
                }
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(updatedStorageKafka);

                LOGGER.info("Updating with changed storage type");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the Volumes and PVCs were not changed
                assertPVCs(context, CLUSTER_NAME + "-kafka");
                assertVolumes(context, CLUSTER_NAME + "-kafka");
                async.flag();
            })));
    }


    private void assertPVCs(VertxTestContext context, String podSetName) {
        context.verify(() -> {
            StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(podSetName).get();
            assertThat(sps, is(notNullValue()));
            List<PersistentVolumeClaimVolumeSource> persistentVolumeClaim = new ArrayList<>();
            // We need to loop through the pods to make sure they have the right values
            sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                pod.getSpec().getVolumes().forEach(volume -> {
                    if (volume.getPersistentVolumeClaim() != null) {
                        persistentVolumeClaim.add(volume.getPersistentVolumeClaim());
                    }
                });
            });
            assertThat(persistentVolumeClaim.size(), is(3));
        });
    }


    private void assertVolumes(VertxTestContext context, String podSetName) {
        context.verify(() -> {
            StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(podSetName).get();
            assertThat(sps, is(notNullValue()));
            sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                List<Volume> volumes = pod.getSpec().getVolumes();
                assertThat(volumes.size(), is(7));
                assertThat(volumes.size(), is(7));
            });
        });
    }

    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @Test
    public void testReconcileUpdatesKafkaWithChangedDeleteClaim(VertxTestContext context) {
        assumeTrue(kafkaStorage instanceof PersistentClaimStorage, "Kafka delete claims do not apply to non-persistent volumes");

        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Map<String, String> zkLabels = new HashMap<>();
        zkLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        zkLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        zkLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-zookeeper");

        AtomicReference<Set<String>> kafkaPvcs = new AtomicReference<>();
        AtomicReference<Set<String>> zkPvcs = new AtomicReference<>();
        AtomicBoolean originalKafkaDeleteClaim = new AtomicBoolean();


        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                kafkaPvcs.set(client.persistentVolumeClaims().inNamespace(NAMESPACE).withLabels(kafkaLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                zkPvcs.set(client.persistentVolumeClaims().inNamespace(NAMESPACE).withLabels(zkLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                originalKafkaDeleteClaim.set(deleteClaim(kafkaStorage));

                // Try to update the storage class
                Kafka updatedStorageKafka = new KafkaBuilder(cluster).editSpec().editKafka()
                        .withNewPersistentClaimStorage()
                        .withSize("123")
                        .withStorageClass("foo")
                        .withDeleteClaim(!originalKafkaDeleteClaim.get())
                        .endPersistentClaimStorage().endKafka().endSpec().build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), updatedStorageKafka);
                LOGGER.info("Updating with changed delete claim");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // check that the new delete-claim annotation is on the PVCs
                for (String pvcName: kafkaPvcs.get()) {
                    assertThat(client.persistentVolumeClaims().inNamespace(NAMESPACE).withName(pvcName).get()
                                    .getMetadata().getAnnotations(),
                            hasEntry(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(!originalKafkaDeleteClaim.get())));
                }
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                LOGGER.info("Reconciling again -> delete");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testReconcileKafkaScaleDown(VertxTestContext context) {
        assumeTrue(kafkaReplicas > 1, "Skipping scale down test because there's only 1 broker");

        int scaleDownTo = kafkaReplicas - 1;
        // final ordinal will be deleted
        String deletedPod = KafkaResources.kafkaPodName(CLUSTER_NAME, scaleDownTo);

        AtomicInteger brokersInternalCertsCount = new AtomicInteger();

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                brokersInternalCertsCount.set(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get()
                        .getData()
                        .size());

                assertThat(client.pods().inNamespace(NAMESPACE).withName(deletedPod).get(), is(notNullValue()));

                Kafka scaledDownCluster = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleDownTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), scaledDownCluster);

                LOGGER.info("Scaling down to {} Kafka pods", scaleDownTo);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get().getSpec().getPods().size(),
                        is(scaleDownTo));
                assertThat("Expected pod " + deletedPod + " to have been deleted",
                        client.pods().inNamespace(NAMESPACE).withName(deletedPod).get(),
                        is(nullValue()));

                // removing one pod, the related private and public keys, keystore and password (4 entries) should not be in the Secrets
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get()
                                .getData(),
                        aMapWithSize(brokersInternalCertsCount.get() - 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testReconcileKafkaScaleUp(VertxTestContext context) {
        AtomicInteger brokersInternalCertsCount = new AtomicInteger();

        Checkpoint async = context.checkpoint();
        int scaleUpTo = kafkaReplicas + 1;
        String newPod = KafkaResources.kafkaPodName(CLUSTER_NAME, kafkaReplicas);

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                brokersInternalCertsCount.set(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get()
                        .getData()
                        .size());

                assertThat(client.pods().inNamespace(NAMESPACE).withName(newPod).get(), is(nullValue()));

                Kafka scaledUpKafka = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleUpTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), scaledUpKafka);

                LOGGER.info("Scaling up to {} Kafka pods", scaleUpTo);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get().getSpec().getPods().size(),
                        is(scaleUpTo));
                assertThat("Expected pod " + newPod + " to have been created",
                        client.pods().inNamespace(NAMESPACE).withName(newPod).get(),
                        is(notNullValue()));

                // adding one pod, the related private and public keys, keystore and password should be added to the Secrets
                assertThat(client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData(),
                        aMapWithSize(brokersInternalCertsCount.get() + 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }

    @Test
    public void testReconcileResumePartialRoll(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(v -> async.flag());
    }
}
