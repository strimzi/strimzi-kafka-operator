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
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.kafka.Storage.deleteClaim;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final int KAFKA_REPLICAS = 3;

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private Storage kafkaStorage;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    private Kafka cluster;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withStrimziPodSetCrd()
                .withPodController()
                .withDeploymentController()
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
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    /*
     * init is equivalent to a @BeforeEach method
     * since this is a parameterized set, the tests params are only available at test start
     * This must be called before each test
     */

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        cluster = new KafkaBuilder()
                 .withNewMetadata()
                     .withName(CLUSTER_NAME)
                     .withNamespace(namespace)
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

        // Create the initial resources
        Crds.kafkaOperation(client).inNamespace(namespace).resource(cluster).create();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        supplier = supplierWithMocks();
        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        return new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(), ResourceUtils.kafkaAgentClientProvider(),
                ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), 2_000);
    }


    private Future<Void> initialReconcile(VertxTestContext context) {
        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(KafkaResources.kafkaComponentName(CLUSTER_NAME)).get();
                assertThat(sps, is(notNullValue()));

                sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0"));
                    var brokersSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH,
                            CertUtils.getCertificateThumbprint(brokersSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()))
                    ));
                });

                StrimziPodSet zkSps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(KafkaResources.zookeeperComponentName(CLUSTER_NAME)).get();
                zkSps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0"));
                    var zooKeeperSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.zookeeperSecretName(CLUSTER_NAME)).get();
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH,
                            CertUtils.getCertificateThumbprint(zooKeeperSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()))
                    ));
                });
                assertThat(client.configMaps().inNamespace(namespace).withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.zookeeperSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
            })));
    }

    /** Create a cluster from a Kafka */
    @Test
    public void testReconcile(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(i -> { }))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
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
                KafkaResources.secretName(CLUSTER_NAME));
    }

    /**
     * Test the operator re-creates secrets if they get deleted
     */
    private void initialReconcileThenDeleteSecretsThenReconcile(VertxTestContext context, String... secrets) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    client.secrets().inNamespace(namespace).withName(secret).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    client.secrets().inNamespace(namespace).withName(secret).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                    assertThat("Expected secret " + secret + " to not exist",
                            client.secrets().inNamespace(namespace).withName(secret).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    assertThat("Expected secret " + secret + " to have been recreated",
                            client.secrets().inNamespace(namespace).withName(secret).get(), is(notNullValue()));
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
                    client.services().inNamespace(namespace).withName(service).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    client.services().inNamespace(namespace).withName(service).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                    assertThat("Expected service " + service + " to be not exist",
                            client.services().inNamespace(namespace).withName(service).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service: services) {
                    assertThat("Expected service " + service + " to have been recreated",
                            client.services().inNamespace(namespace).withName(service).get(), is(notNullValue()));
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
                supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                assertThat("Expected sts " + podSetName + " should not exist",
                        supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).get(), is(nullValue()));

                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("Expected sts " + podSetName + " should have been re-created",
                        supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-kafka").get(), is(notNullValue()));
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
                kafkaAssembly(namespace, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), patchedPersistenceKafka);

                LOGGER.info("Updating with changed storage class");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
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
                kafkaAssembly(namespace, CLUSTER_NAME).patch(updatedStorageKafka);

                LOGGER.info("Updating with changed storage type");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
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
            client.persistentVolumeClaims().inNamespace(namespace).list().getItems().forEach(persistentVolumeClaim -> {
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
            StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).get();
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
                kafkaPvcs.set(client.persistentVolumeClaims().inNamespace(namespace).withLabels(kafkaLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                zkPvcs.set(client.persistentVolumeClaims().inNamespace(namespace).withLabels(zkLabels).list().getItems()
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
                kafkaAssembly(namespace, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), updatedStorageKafka);
                LOGGER.info("Updating with changed delete claim");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // check that the new delete-claim annotation is on the PVCs
                for (String pvcName: kafkaPvcs.get()) {
                    assertThat(client.persistentVolumeClaims().inNamespace(namespace).withName(pvcName).get()
                                    .getMetadata().getAnnotations(),
                            hasEntry(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(!originalKafkaDeleteClaim.get())));
                }
                kafkaAssembly(namespace, CLUSTER_NAME).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                LOGGER.info("Reconciling again -> delete");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
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
                brokersInternalCertsCount.set(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get()
                        .getData()
                        .size());

                assertThat(client.pods().inNamespace(namespace).withName(deletedPod).get(), is(notNullValue()));

                Kafka scaledDownCluster = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleDownTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(namespace, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), scaledDownCluster);

                LOGGER.info("Scaling down to {} Kafka pods", scaleDownTo);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(CLUSTER_NAME + "-kafka").get().getSpec().getPods().size(),
                        is(scaleDownTo));
                assertThat("Expected pod " + deletedPod + " to have been deleted",
                        client.pods().inNamespace(namespace).withName(deletedPod).get(),
                        is(nullValue()));

                // removing one pod, the related private and public keys, keystore and password (4 entries) should not be in the Secrets
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get()
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
                brokersInternalCertsCount.set(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get()
                        .getData()
                        .size());

                assertThat(client.pods().inNamespace(namespace).withName(newPod).get(), is(nullValue()));

                Kafka scaledUpKafka = new KafkaBuilder(cluster)
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleUpTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(namespace, CLUSTER_NAME).patch(PatchContext.of(PatchType.JSON), scaledUpKafka);

                LOGGER.info("Scaling up to {} Kafka pods", scaleUpTo);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-kafka").get().getSpec().getPods().size(),
                        is(scaleUpTo));
                assertThat("Expected pod " + newPod + " to have been created",
                        client.pods().inNamespace(namespace).withName(newPod).get(),
                        is(notNullValue()));

                // adding one pod, the related private and public keys, keystore and password should be added to the Secrets
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData(),
                        aMapWithSize(brokersInternalCertsCount.get() + 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }
}
