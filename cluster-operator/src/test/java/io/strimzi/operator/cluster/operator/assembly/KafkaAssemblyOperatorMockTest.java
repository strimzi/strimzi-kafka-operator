/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static WorkerExecutor sharedWorkerExecutor;

    private static final int KAFKA_REPLICAS = 3;
    private Storage kafkaStorage;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    private Kafka cluster;

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
                         .withReplicas(KAFKA_REPLICAS)
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

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        supplier = supplierWithMocks();
        podSetController = new StrimziPodSetController(NAMESPACE, Labels.EMPTY, supplier.kafkaOperator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, ClusterOperatorConfig.DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        return new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), 2_000);
    }


    private Future<Void> initialReconcile(VertxTestContext context) {
        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).get();
                assertThat(sps, is(notNullValue()));

                sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    var brokersSecret = client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(KafkaCluster.ANNO_STRIMZI_SERVER_CERT_HASH,
                            CertUtils.getCertificateThumbprint(brokersSecret, Ca.secretEntryNameForPod(pod.getMetadata().getName(), Ca.SecretEntry.CRT))
                    ));
                });

                StrimziPodSet zkSps = supplier.strimziPodSetOperator.client().inNamespace(NAMESPACE).withName(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).get();
                zkSps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                });
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

    @Test
    public void testReconcileUpdatesKafkaStorageType(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                Kafka updatedStorageKafka = null;
                if (cluster.getSpec().getKafka().getStorage() instanceof PersistentClaimStorage) {
                    updatedStorageKafka = new KafkaBuilder(cluster)
                            .editSpec()
                                .editKafka()
                                    .withNewEphemeralStorage()
                                    .endEphemeralStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else {
                    context.failNow(new Exception("If storage is not persistent, something has gone wrong"));
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

        assertThat(kafkaStorage.getType(), is("persistent-claim"));

        context.verify(() -> {
            List<PersistentVolumeClaim> pvc = new ArrayList<>();
            client.persistentVolumeClaims().inNamespace(NAMESPACE).list().getItems().forEach(persistentVolumeClaim -> {
                if (persistentVolumeClaim.getMetadata().getName().startsWith("data-" + podSetName)) {
                    pvc.add(persistentVolumeClaim);
                    assertThat(persistentVolumeClaim.getSpec().getStorageClassName(), is("foo"));
                    assertThat(persistentVolumeClaim.getSpec().getResources().getRequests().toString(), is("{storage=123}"));
                }
            });
            assertThat(pvc.size(), is(3));
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
                            hasEntry(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(!originalKafkaDeleteClaim.get())));
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
        int scaleDownTo = KAFKA_REPLICAS - 1;
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
                assertThat(Crds.strimziPodSetOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get().getSpec().getPods().size(),
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
        int scaleUpTo = KAFKA_REPLICAS + 1;
        String newPod = KafkaResources.kafkaPodName(CLUSTER_NAME, KAFKA_REPLICAS);

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
