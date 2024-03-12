/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatus;
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
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorWithPoolsMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorWithPoolsMockTest.class);

    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private Vertx vertx;
    private WorkerExecutor sharedWorkerExecutor;
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
                .withDeploymentController()
                .withServiceController()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void init(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withConfig(new HashMap<>())
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
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

        KafkaNodePool poolA = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("pool-a")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withGeneration(1L)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.BROKER)
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"))).build())
                .endSpec()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("pool-b")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withGeneration(1L)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(2)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.BROKER)
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("6"))).build())
                .endSpec()
                .build();

        // Create the initial resources
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(poolA).create();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(poolB).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(cluster).create();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        supplier = supplierWithMocks();
        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), VERSIONS)
                .with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "10000")
                .build();
        operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
        sharedWorkerExecutor.close();
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
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
                StrimziPodSet spsPoolA = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-a").get();
                assertThat(spsPoolA, is(notNullValue()));

                spsPoolA.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    var brokersSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH,
                            CertUtils.getCertificateThumbprint(brokersSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()))
                    ));
                });

                StrimziPodSet spsPoolB = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-b").get();
                assertThat(spsPoolB, is(notNullValue()));

                spsPoolB.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    var brokersSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH,
                            CertUtils.getCertificateThumbprint(brokersSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()))
                    ));
                });

                StrimziPodSet zkSps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(KafkaResources.zookeeperComponentName(CLUSTER_NAME)).get();
                zkSps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                    assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
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
        List<String> secrets = List.of(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.kafkaSecretName(CLUSTER_NAME),
                KafkaResources.zookeeperSecretName(CLUSTER_NAME),
                KafkaResources.secretName(CLUSTER_NAME));

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

    @Test
    public void testReconcileReplacesDeletedKafkaServices(VertxTestContext context) {
        List<String> services = List.of(KafkaResources.bootstrapServiceName(CLUSTER_NAME), KafkaResources.brokersServiceName(CLUSTER_NAME));

        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service : services) {
                    client.services().inNamespace(namespace).withName(service).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
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
    public void testReconcileReplacesDeletedKafkaPodSet(VertxTestContext context) {
        String podSetName = CLUSTER_NAME + "-pool-a";

        Checkpoint async = context.checkpoint();
        initialReconcile(context)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
                    Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(podSetName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                    assertThat("Expected sps " + podSetName + " should not exist",
                            supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).get(), is(nullValue()));

                    LOGGER.info("Reconciling again -> update");
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat("Expected sps " + podSetName + " should have been re-created",
                            supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).get(), is(notNullValue()));
                    async.flag();
                })));
    }

    @Test
    public void testReconcileUpdatesKafkaPersistentVolumes(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                client.persistentVolumeClaims().inNamespace(namespace).list().getItems().forEach(pvc -> {
                    if (pvc.getMetadata().getName().startsWith(CLUSTER_NAME + "-pool-"))    {
                        assertThat(pvc.getSpec().getStorageClassName(), is("gp99"));
                    }
                });

                // Try to update the storage class
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp100").build())
                            .endJbodStorage()
                        .endSpec()
                        .build());
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp100").build())
                            .endJbodStorage()
                        .endSpec()
                        .build());

                LOGGER.info("Updating pools with changed storage class");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the storage class was not changed
                client.persistentVolumeClaims().inNamespace(namespace).list().getItems().forEach(pvc -> {
                    if (pvc.getMetadata().getName().startsWith(CLUSTER_NAME + "-pool-"))    {
                        assertThat(pvc.getSpec().getStorageClassName(), is("gp99"));
                    }
                });

                async.flag();
            })));
    }

    @Test
    public void testReconcileUpdatesKafkaStorageType(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Try to update the storage class
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endSpec()
                        .build());
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endSpec()
                        .build());

                LOGGER.info("Updating pools with changed storage type");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the Volumes and PVCs were not changed
                assertPVCs(context, CLUSTER_NAME + "-pool-a", 3, "100Gi");
                assertPVCs(context, CLUSTER_NAME + "-pool-b", 2, "200Gi");

                async.flag();
            })));
    }


    private void assertPVCs(VertxTestContext context, String podSetName, int expectedPvcs, String expectedSize) {
        context.verify(() -> {
            List<PersistentVolumeClaim> pvc = new ArrayList<>();
            client.persistentVolumeClaims().inNamespace(namespace).list().getItems().forEach(persistentVolumeClaim -> {
                if (persistentVolumeClaim.getMetadata().getName().startsWith("data-0-" + podSetName)) {
                    pvc.add(persistentVolumeClaim);
                    assertThat(persistentVolumeClaim.getSpec().getStorageClassName(), is("gp99"));
                    assertThat(persistentVolumeClaim.getSpec().getResources().getRequests().get("storage").toString(), is(expectedSize));
                }
            });

            assertThat(pvc.size(), is(expectedPvcs));
        });
    }

    @Test
    public void testReconcileUpdatesKafkaWithChangedDeleteClaim(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                client.persistentVolumeClaims().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().forEach(pvc -> assertThat(pvc.getMetadata().getOwnerReferences(), is(List.of())));

                // Try to update the storage class
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").withDeleteClaim(true).build())
                            .endJbodStorage()
                        .endSpec()
                        .build());
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").withStorageClass("gp99").withDeleteClaim(true).build())
                            .endJbodStorage()
                        .endSpec()
                        .build());

                LOGGER.info("Updating pools with changed delete claim");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // check that the new delete-claim annotation is on the PVCs
                client.persistentVolumeClaims().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().forEach(pvc -> {
                    assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
                    assertThat(pvc.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(true)));
                });

                Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

                LOGGER.info("Reconciling again -> delete");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    @Test
    public void testReconcileKafkaScaleDown(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(20));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(5));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-a-2").get(), is(notNullValue()));

                // Scale down one of the pools
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withReplicas(2)
                        .endSpec()
                        .build());

                LOGGER.info("Scaling down pool-a to 2 pods");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(16));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(4));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-a-2").get(), is(nullValue()));

                KafkaNodePool poolA = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").get();
                assertThat(poolA.getStatus().getReplicas(), is(2));
                assertThat(poolA.getStatus().getNodeIds(), is(List.of(0, 1)));
                assertThat(poolA.getStatus().getRoles().size(), is(1));
                assertThat(poolA.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool poolB = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").get();
                assertThat(poolB.getStatus().getReplicas(), is(2));
                assertThat(poolB.getStatus().getNodeIds(), is(List.of(3, 4)));
                assertThat(poolB.getStatus().getRoles().size(), is(1));
                assertThat(poolB.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                async.flag();
            })));
    }

    @Test
    public void testReconcileKafkaScaleUp(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(20));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(5));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-a-5").get(), is(nullValue()));

                // Scale down one of the pools
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withReplicas(4)
                        .endSpec()
                        .build());

                LOGGER.info("Scaling up pool-a to 4 pods");
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(24));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(6));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-a-5").get(), is(notNullValue()));

                KafkaNodePool poolA = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").get();
                assertThat(poolA.getStatus().getReplicas(), is(4));
                assertThat(poolA.getStatus().getNodeIds(), is(List.of(0, 1, 2, 5)));
                assertThat(poolA.getStatus().getRoles().size(), is(1));
                assertThat(poolA.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool poolB = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").get();
                assertThat(poolB.getStatus().getReplicas(), is(2));
                assertThat(poolB.getStatus().getNodeIds(), is(List.of(3, 4)));
                assertThat(poolB.getStatus().getRoles().size(), is(1));
                assertThat(poolB.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                async.flag();
            })));
    }

    @Test
    public void testReconcileAddPool(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(20));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(5));

                KafkaNodePool poolC = new KafkaNodePoolBuilder()
                        .withNewMetadata()
                            .withName("pool-c")
                            .withNamespace(namespace)
                            .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                            .withGeneration(1L)
                        .endMetadata()
                        .withNewSpec()
                            .withReplicas(2)
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("300Gi").withStorageClass("gp99").build())
                            .endJbodStorage()
                            .withRoles(ProcessRoles.BROKER)
                            .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("8"))).build())
                        .endSpec()
                        .build();

                LOGGER.info("Creating new node pool");
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(poolC).create();
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Assert that the new pool is added
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(28));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(7));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-c-5").get(), is(notNullValue()));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-c-6").get(), is(notNullValue()));

                Kafka kafka = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(kafka.getStatus().getKafkaNodePools().size(), is(3));
                assertThat(kafka.getStatus().getKafkaNodePools().stream().map(UsedNodePoolStatus::getName).toList(), hasItems("pool-a", "pool-b", "pool-c"));

                KafkaNodePool poolA = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").get();
                assertThat(poolA.getStatus().getReplicas(), is(3));
                assertThat(poolA.getStatus().getNodeIds(), is(List.of(0, 1, 2)));
                assertThat(poolA.getStatus().getRoles().size(), is(1));
                assertThat(poolA.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool poolB = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").get();
                assertThat(poolB.getStatus().getReplicas(), is(2));
                assertThat(poolB.getStatus().getNodeIds(), is(List.of(3, 4)));
                assertThat(poolB.getStatus().getRoles().size(), is(1));
                assertThat(poolB.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool poolC = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-c").get();
                assertThat(poolC.getStatus().getReplicas(), is(2));
                assertThat(poolC.getStatus().getNodeIds(), is(List.of(5, 6)));
                assertThat(poolC.getStatus().getRoles().size(), is(1));
                assertThat(poolC.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                async.flag();
            })));
    }

    @Test
    public void testReconcileAndRemovePool(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        KafkaNodePool additionalPool = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("pool-c")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withGeneration(1L)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(2)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("300Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.BROKER)
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("8"))).build())
                .endSpec()
                .build();

        LOGGER.info("Creating additional node pool");
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(additionalPool).create();

        initialReconcile(context)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Assert that the new pool is added
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(28));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(7));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-c-5").get(), is(notNullValue()));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-c-6").get(), is(notNullValue()));

                Kafka kafka = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(kafka.getStatus().getKafkaNodePools().size(), is(3));
                assertThat(kafka.getStatus().getKafkaNodePools().stream().map(UsedNodePoolStatus::getName).toList(), hasItems("pool-a", "pool-b", "pool-c"));

                KafkaNodePool poolA = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").get();
                assertThat(poolA.getStatus().getReplicas(), is(3));
                assertThat(poolA.getStatus().getNodeIds(), is(List.of(0, 1, 2)));
                assertThat(poolA.getStatus().getRoles().size(), is(1));
                assertThat(poolA.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool poolB = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").get();
                assertThat(poolB.getStatus().getReplicas(), is(2));
                assertThat(poolB.getStatus().getNodeIds(), is(List.of(3, 4)));
                assertThat(poolB.getStatus().getRoles().size(), is(1));
                assertThat(poolB.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool poolC = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-c").get();
                assertThat(poolC.getStatus().getReplicas(), is(2));
                assertThat(poolC.getStatus().getNodeIds(), is(List.of(5, 6)));
                assertThat(poolC.getStatus().getRoles().size(), is(1));
                assertThat(poolC.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                // Remove pool-b
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").withPropagationPolicy(DeletionPropagation.BACKGROUND).withGracePeriod(0L).delete();
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-b").waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Assert that pool was removed
                assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get().getData().size(), is(20));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(5));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-b-3").get(), is(nullValue()));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-pool-b-4").get(), is(nullValue()));

                Kafka kafka = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(kafka.getStatus().getKafkaNodePools().size(), is(2));
                assertThat(kafka.getStatus().getKafkaNodePools().stream().map(UsedNodePoolStatus::getName).toList(), hasItems("pool-a", "pool-c"));

                KafkaNodePool poolA = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-a").get();
                assertThat(poolA.getStatus().getReplicas(), is(3));
                assertThat(poolA.getStatus().getNodeIds(), is(List.of(0, 1, 2)));

                KafkaNodePool poolC = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("pool-c").get();
                assertThat(poolC.getStatus().getReplicas(), is(2));
                assertThat(poolC.getStatus().getNodeIds(), is(List.of(5, 6)));

                async.flag();
            })));
    }
}
