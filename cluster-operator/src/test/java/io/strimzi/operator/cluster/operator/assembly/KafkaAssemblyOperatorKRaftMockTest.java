/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
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
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorKRaftMockTest {
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");

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
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
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
                .endSpec()
                .withNewStatus()
                    .withClusterId("CLUSTER-ID")
                .endStatus()
                .build();

        KafkaNodePool controllers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("controllers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[0-9]"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER)
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"))).build())
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("brokers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[10-99]"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.BROKER)
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("6"))).build())
                .endSpec()
                .build();

        // Create the initial resources
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(controllers).create();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(brokers).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(cluster).create();

        // We have to update the status to store the Kafka Cluster ID in it.
        // This is needed to keep the resources in sync with the Kafka Admin API mocks.
        Crds.kafkaOperation(client).resource(cluster).updateStatus();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        supplier = supplierWithMocks();
        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), VERSIONS)
                .with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "10000")
                .build();
        operator = new KafkaAssemblyOperator(vertx, pfa, CERT_MANAGER, PASSWORD_GENERATOR, supplier, config);
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        return new ResourceOperatorSupplier(vertx, client, ResourceUtils.adminClientProvider(),
                ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(),
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
        sharedWorkerExecutor.close();
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    /**
     * Does a basic check of the resources created / updated by a reconciliation
     */
    private void basicCheck()    {
        // Check pod sets, pods and their resources
        StrimziPodSet controllers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controllers").get();
        assertThat(controllers, is(notNullValue()));

        controllers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(desiredPod -> {
            assertThat(desiredPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
            assertThat(desiredPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

            Pod actualPod = client.pods().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get();
            assertThat(actualPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
            assertThat(actualPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

            Secret certSecret = client.secrets().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get();
            assertThat(desiredPod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(certSecret, Ca.SecretEntry.CRT.asKey(desiredPod.getMetadata().getName()))));
            assertThat(actualPod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(certSecret, Ca.SecretEntry.CRT.asKey(desiredPod.getMetadata().getName()))));

            assertThat(client.configMaps().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get(), is(notNullValue()));
        });

        StrimziPodSet brokers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers").get();
        assertThat(brokers, is(notNullValue()));

        brokers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(desiredPod -> {
            assertThat(desiredPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
            assertThat(desiredPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

            Pod actualPod = client.pods().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get();
            assertThat(actualPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
            assertThat(actualPod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

            Secret certSecret = client.secrets().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get();
            assertThat(desiredPod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(certSecret, Ca.SecretEntry.CRT.asKey(desiredPod.getMetadata().getName()))));
            assertThat(actualPod.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(certSecret, Ca.SecretEntry.CRT.asKey(desiredPod.getMetadata().getName()))));

            assertThat(client.configMaps().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get(), is(notNullValue()));
        });

        // Check CA Secrets
        assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME)).get(), is(notNullValue()));
        assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME)).get(), is(notNullValue()));
        assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clusterCaKeySecretName(CLUSTER_NAME)).get(), is(notNullValue()));
        assertThat(client.secrets().inNamespace(namespace).withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)).get(), is(notNullValue()));

        // Check the Kafka resource status
        Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
        assertThat(k.getStatus(), is(notNullValue()));
        assertThat(Kafka.isReady().test(k), is(true));
        assertThat(k.getStatus().getKafkaMetadataVersion(), startsWith(VERSIONS.defaultVersion().metadataVersion() + "-IV"));
        assertThat(k.getStatus().getKafkaVersion(), is(VERSIONS.defaultVersion().version()));
        assertThat(k.getStatus().getOperatorLastSuccessfulVersion(), is(KafkaAssemblyOperator.OPERATOR_VERSION));
        assertThat(k.getStatus().getKafkaMetadataState().toValue(), is("KRaft"));
        assertThat(k.getStatus().getKafkaNodePools().stream().map(UsedNodePoolStatus::getName).toList(), is(List.of("brokers", "controllers")));
        assertThat(k.getStatus().getRegisteredNodeIds().size(), is(6));
        assertThat(k.getStatus().getRegisteredNodeIds(), hasItems(0, 1, 2, 10, 11, 12));
    }

    /**
     * Test regular reconciliation to create and reconcile a Kafka cluster together with some basic checks.
     *
     * @param context   Test context
     */
    @Test
    public void testReconcile(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(this::basicCheck)))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> {
                basicCheck();

                async.flag();
            }));
    }

    /**
     * Checks that the secrets are recreated during reconciliation
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileReplacesAllDeletedSecrets(VertxTestContext context) {
        List<String> secrets = List.of(KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME),
                CLUSTER_NAME + "-controllers-0",
                CLUSTER_NAME + "-controllers-1",
                CLUSTER_NAME + "-controllers-2",
                CLUSTER_NAME + "-brokers-10",
                CLUSTER_NAME + "-brokers-11",
                CLUSTER_NAME + "-brokers-12",
                KafkaResources.clusterOperatorCertsSecretName(CLUSTER_NAME));

        Checkpoint async = context.checkpoint();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    for (String secret: secrets) {
                        client.secrets().inNamespace(namespace).withName(secret).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                        client.secrets().inNamespace(namespace).withName(secret).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                        assertThat("Expected secret " + secret + " to not exist", client.secrets().inNamespace(namespace).withName(secret).get(), is(nullValue()));
                    }
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    for (String secret: secrets) {
                        assertThat("Expected secret " + secret + " to have been recreated", client.secrets().inNamespace(namespace).withName(secret).get(), is(notNullValue()));
                    }

                    async.flag();
                })));
    }

    /**
     * Checks that the services are recreated during reconciliation
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileReplacesDeletedKafkaServices(VertxTestContext context) {
        List<String> services = List.of(KafkaResources.bootstrapServiceName(CLUSTER_NAME), KafkaResources.brokersServiceName(CLUSTER_NAME));

        Checkpoint async = context.checkpoint();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service : services) {
                    client.services().inNamespace(namespace).withName(service).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
                    client.services().inNamespace(namespace).withName(service).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                    assertThat("Expected service " + service + " to be not exist", client.services().inNamespace(namespace).withName(service).get(), is(nullValue()));
                }
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service: services) {
                    assertThat("Expected service " + service + " to have been recreated", client.services().inNamespace(namespace).withName(service).get(), is(notNullValue()));
                }

                async.flag();
            })));
    }

    /**
     * Checks that a deleted StrimziPodSets are recreated during the reconciliation
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileReplacesDeletedKafkaPodSet(VertxTestContext context) {
        String podSetName = CLUSTER_NAME + "-brokers";

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
                    Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(podSetName).waitUntilCondition(Objects::isNull, 10_000, TimeUnit.MILLISECONDS);
                    assertThat("Expected sps " + podSetName + " should not exist", supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).get(), is(nullValue()));
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat("Expected sps " + podSetName + " should have been re-created", supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(podSetName).get(), is(notNullValue()));

                    async.flag();
                })));
    }

    /**
     * Checks that Storage Class changes are rejected
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileUpdatesKafkaPersistentVolumes(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = client.persistentVolumeClaims().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)).list().getItems();
                assertThat(pvcs.size(), is(6));
                pvcs.forEach(pvc -> assertThat(pvc.getSpec().getStorageClassName(), is("gp99")));

                // Try to update the storage class
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp100").build())
                            .endJbodStorage()
                        .endSpec()
                        .build());
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp100").build())
                            .endJbodStorage()
                        .endSpec()
                        .build());
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the storage class was not changed
                List<PersistentVolumeClaim> pvcs = client.persistentVolumeClaims().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)).list().getItems();
                assertThat(pvcs.size(), is(6));
                pvcs.forEach(pvc -> assertThat(pvc.getSpec().getStorageClassName(), is("gp99")));

                async.flag();
            })));
    }

    /**
     * Checks that changes from persistent to ephemeral storage are rejected
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileUpdatesKafkaStorageType(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Try to update the storage class
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endSpec()
                        .build());
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endSpec()
                        .build());
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the Volumes and PVCs were not changed
                List<PersistentVolumeClaim> pvcs = client.persistentVolumeClaims().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)).list().getItems();
                assertThat(pvcs.size(), is(6));
                pvcs.forEach(pvc -> assertThat(pvc.getSpec().getStorageClassName(), is("gp99")));

                // Check the PVCs are still in use
                StrimziPodSet controllers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controllers").get();
                controllers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(desiredPod -> {
                    Pod actualPod = client.pods().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get();
                    assertThat(desiredPod.getSpec().getVolumes().stream().filter(vol -> "data-0".equals(vol.getName())).findFirst().orElseThrow(), is(notNullValue()));
                    assertThat(actualPod.getSpec().getVolumes().stream().filter(vol -> "data-0".equals(vol.getName())).findFirst().orElseThrow(), is(notNullValue()));
                });

                StrimziPodSet brokers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers").get();
                brokers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(desiredPod -> {
                    Pod actualPod = client.pods().inNamespace(namespace).withName(desiredPod.getMetadata().getName()).get();
                    assertThat(desiredPod.getSpec().getVolumes().stream().filter(vol -> "data-0".equals(vol.getName())).findFirst().orElseThrow(), is(notNullValue()));
                    assertThat(actualPod.getSpec().getVolumes().stream().filter(vol -> "data-0".equals(vol.getName())).findFirst().orElseThrow(), is(notNullValue()));
                });

                async.flag();
            })));
    }

    /**
     * Checks that the delete claim is updated
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileUpdatesKafkaWithChangedDeleteClaim(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                List<PersistentVolumeClaim> pvcs = client.persistentVolumeClaims().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)).list().getItems();
                assertThat(pvcs.size(), is(6));
                pvcs.forEach(pvc -> {
                    assertThat(pvc.getMetadata().getOwnerReferences(), is(List.of()));
                    assertThat(pvc.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(false)));
                });

                // Try to update the storage class
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").withDeleteClaim(true).build())
                            .endJbodStorage()
                        .endSpec()
                        .build());
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").withStorageClass("gp99").withDeleteClaim(true).build())
                            .endJbodStorage()
                        .endSpec()
                        .build());
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // check that the new delete-claim annotation is on the PVCs
                List<PersistentVolumeClaim> pvcs = client.persistentVolumeClaims().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)).list().getItems();
                assertThat(pvcs.size(), is(6));
                pvcs.forEach(pvc -> {
                    assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
                    assertThat(pvc.getMetadata().getOwnerReferences().get(0).getKind(), is("KafkaNodePool"));
                    assertThat(pvc.getMetadata().getAnnotations(), hasEntry(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(true)));
                });

                async.flag();
            })));
    }

    /**
     * Checks that scaling the Kafka cluster up and down works
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileKafkaScaleUpAndDown(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withLabels(Labels.fromMap(kafkaLabels).withStrimziComponentType("kafka").toMap()).list().getItems().size(), is(6));
                assertThat(client.secrets().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-13").get(), is(nullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-14").get(), is(nullValue()));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(6));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-13").get(), is(nullValue()));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-14").get(), is(nullValue()));

                // Scale down one of the pools
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withReplicas(5)
                        .endSpec()
                        .build());
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withLabels(Labels.fromMap(kafkaLabels).withStrimziComponentType("kafka").toMap()).list().getItems().size(), is(8));
                assertThat(client.secrets().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-13").get(), is(notNullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-14").get(), is(notNullValue()));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(8));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-13").get(), is(notNullValue()));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-14").get(), is(notNullValue()));

                KafkaNodePool controllers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").get();
                assertThat(controllers.getStatus().getReplicas(), is(3));
                assertThat(controllers.getStatus().getNodeIds(), is(List.of(0, 1, 2)));
                assertThat(controllers.getStatus().getRoles().size(), is(1));
                assertThat(controllers.getStatus().getRoles(), hasItems(ProcessRoles.CONTROLLER));

                KafkaNodePool brokers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").get();
                assertThat(brokers.getStatus().getReplicas(), is(5));
                assertThat(brokers.getStatus().getNodeIds(), is(List.of(10, 11, 12, 13, 14)));
                assertThat(brokers.getStatus().getRoles().size(), is(1));
                assertThat(brokers.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                // Check the Kafka resource status
                Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(k.getStatus().getRegisteredNodeIds().size(), is(8));
                assertThat(k.getStatus().getRegisteredNodeIds(), hasItems(0, 1, 2, 10, 11, 12, 13, 14));

                // Scale down again
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withReplicas(3)
                        .endSpec()
                        .build());
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withLabels(Labels.fromMap(kafkaLabels).withStrimziComponentType("kafka").toMap()).list().getItems().size(), is(6));
                assertThat(client.secrets().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-13").get(), is(nullValue()));
                assertThat(client.secrets().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-14").get(), is(nullValue()));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(6));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-13").get(), is(nullValue()));
                assertThat(client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers-14").get(), is(nullValue()));

                KafkaNodePool controllers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").get();
                assertThat(controllers.getStatus().getReplicas(), is(3));
                assertThat(controllers.getStatus().getNodeIds(), is(List.of(0, 1, 2)));
                assertThat(controllers.getStatus().getRoles().size(), is(1));
                assertThat(controllers.getStatus().getRoles(), hasItems(ProcessRoles.CONTROLLER));

                KafkaNodePool brokers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").get();
                assertThat(brokers.getStatus().getReplicas(), is(3));
                assertThat(brokers.getStatus().getNodeIds(), is(List.of(10, 11, 12)));
                assertThat(brokers.getStatus().getRoles().size(), is(1));
                assertThat(brokers.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                // Check the Kafka resource status
                Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(k.getStatus().getRegisteredNodeIds().size(), is(6));
                assertThat(k.getStatus().getRegisteredNodeIds(), hasItems(0, 1, 2, 10, 11, 12));

                async.flag();
            })));
    }

    /**
     * Checks that adding and removing pools works
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileAddAndRemovePool(VertxTestContext context) {
        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, CLUSTER_NAME + "-kafka");

        Checkpoint async = context.checkpoint();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(client.secrets().inNamespace(namespace).withLabels(Labels.fromMap(kafkaLabels).withStrimziComponentType("kafka").toMap()).list().getItems().size(), is(6));
                List<Pod> initialPods = client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems();
                assertThat(initialPods.size(), is(6));
                for (Pod pod : initialPods) {
                    Secret certSecret = client.secrets().inNamespace(namespace).withName(pod.getMetadata().getName()).get();
                    assertThat(certSecret, is(notNullValue()));
                    assertThat(certSecret.getData().keySet(), hasItems(pod.getMetadata().getName() + ".crt", pod.getMetadata().getName() + ".key"));
                }

                KafkaNodePool newPool = new KafkaNodePoolBuilder()
                        .withNewMetadata()
                            .withName("new-pool")
                            .withNamespace(namespace)
                            .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                            .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[10-99]"))
                        .endMetadata()
                        .withNewSpec()
                            .withReplicas(3)
                            .withNewJbodStorage()
                                .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("300Gi").withStorageClass("gp99").build())
                            .endJbodStorage()
                            .withRoles(ProcessRoles.BROKER)
                            .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("8"))).build())
                        .endSpec()
                        .build();

                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(newPool).create();
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Assert that the new pool is added
                assertThat(client.secrets().inNamespace(namespace).withLabels(Labels.fromMap(kafkaLabels).withStrimziComponentType("kafka").toMap()).list().getItems().size(), is(9));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(9));
                List<String> expectedNewResources = List.of(
                        CLUSTER_NAME + "-new-pool-13",
                        CLUSTER_NAME + "-new-pool-14",
                        CLUSTER_NAME + "-new-pool-15"
                );
                for (String resourceName : expectedNewResources) {
                    assertThat(client.pods().inNamespace(namespace).withName(resourceName).get(), is(notNullValue()));
                    Secret certSecret = client.secrets().inNamespace(namespace).withName(resourceName).get();
                    assertThat(certSecret, is(notNullValue()));
                    assertThat(certSecret.getData().keySet(), hasItems(resourceName + ".crt", resourceName + ".key"));
                }

                Kafka kafka = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(kafka.getStatus().getKafkaNodePools().size(), is(3));
                assertThat(kafka.getStatus().getKafkaNodePools().stream().map(UsedNodePoolStatus::getName).toList(), hasItems("brokers", "controllers", "new-pool"));
                assertThat(kafka.getStatus().getRegisteredNodeIds().size(), is(9));
                assertThat(kafka.getStatus().getRegisteredNodeIds(), hasItems(0, 1, 2, 10, 11, 12, 13, 14, 15));

                KafkaNodePool controllers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").get();
                assertThat(controllers.getStatus().getReplicas(), is(3));
                assertThat(controllers.getStatus().getNodeIds(), is(List.of(0, 1, 2)));
                assertThat(controllers.getStatus().getRoles().size(), is(1));
                assertThat(controllers.getStatus().getRoles(), hasItems(ProcessRoles.CONTROLLER));

                KafkaNodePool brokers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").get();
                assertThat(brokers.getStatus().getReplicas(), is(3));
                assertThat(brokers.getStatus().getNodeIds(), is(List.of(10, 11, 12)));
                assertThat(brokers.getStatus().getRoles().size(), is(1));
                assertThat(brokers.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool newPool = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("new-pool").get();
                assertThat(newPool.getStatus().getReplicas(), is(3));
                assertThat(newPool.getStatus().getNodeIds(), is(List.of(13, 14, 15)));
                assertThat(newPool.getStatus().getRoles().size(), is(1));
                assertThat(newPool.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                // Delete the node pool again
                Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("new-pool").delete();
            })))
            .compose(v -> operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Assert that the new pool is deleted
                assertThat(client.secrets().inNamespace(namespace).withLabels(Labels.fromMap(kafkaLabels).withStrimziComponentType("kafka").toMap()).list().getItems().size(), is(6));
                assertThat(client.pods().inNamespace(namespace).withLabels(kafkaLabels).list().getItems().size(), is(6));
                List<String> expectedDeletedResources = List.of(
                        CLUSTER_NAME + "-new-pool-13",
                        CLUSTER_NAME + "-new-pool-14",
                        CLUSTER_NAME + "-new-pool-15"
                );
                for (String resourceName : expectedDeletedResources) {
                    assertThat(client.pods().inNamespace(namespace).withName(resourceName).get(), is(nullValue()));
                    assertThat(client.secrets().inNamespace(namespace).withName(resourceName).get(), is(nullValue()));
                }

                Kafka kafka = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                assertThat(kafka.getStatus().getKafkaNodePools().size(), is(2));
                assertThat(kafka.getStatus().getKafkaNodePools().stream().map(UsedNodePoolStatus::getName).toList(), hasItems("brokers", "controllers"));
                assertThat(kafka.getStatus().getRegisteredNodeIds().size(), is(6));
                assertThat(kafka.getStatus().getRegisteredNodeIds(), hasItems(0, 1, 2, 10, 11, 12));

                KafkaNodePool controllers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("controllers").get();
                assertThat(controllers.getStatus().getReplicas(), is(3));
                assertThat(controllers.getStatus().getNodeIds(), is(List.of(0, 1, 2)));
                assertThat(controllers.getStatus().getRoles().size(), is(1));
                assertThat(controllers.getStatus().getRoles(), hasItems(ProcessRoles.CONTROLLER));

                KafkaNodePool brokers = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").get();
                assertThat(brokers.getStatus().getReplicas(), is(3));
                assertThat(brokers.getStatus().getNodeIds(), is(List.of(10, 11, 12)));
                assertThat(brokers.getStatus().getRoles().size(), is(1));
                assertThat(brokers.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));

                KafkaNodePool newPool = Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("new-pool").get();
                assertThat(newPool, is(nullValue()));

                async.flag();
            })));
    }

    /**
     * Tests how the KRaft controller-only nodes have their configuration changes tracked using a Pod annotations. The
     * annotation on controller-only pods should change when the controller-relevant config is changed. On broker pods
     * it should never change. To test this, the test does 3 reconciliations:
     *     - First initial one to establish the pods and collects the annotations
     *     - Second with change that is not relevant to controllers => annotations should be the same for all nodes as
     *       before
     *     - Third with change to a controller-relevant option => annotations for controller nodes should change, for
     *       broker nodes should be the same
     *
     * @param context   Test context
     */
    @Test
    public void testReconcileWithControllerRelevantConfigChange(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        Map<String, String> brokerConfigurationAnnotations = new HashMap<>();

        operator.reconcile(new Reconciliation("initial-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Collect the configuration annotations
                    StrimziPodSet spsControllers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controllers").get();
                    assertThat(spsControllers, is(notNullValue()));

                    spsControllers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> brokerConfigurationAnnotations.put(pod.getMetadata().getName(), pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH)));

                    StrimziPodSet spsBrokers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers").get();
                    assertThat(spsBrokers, is(notNullValue()));

                    spsBrokers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> brokerConfigurationAnnotations.put(pod.getMetadata().getName(), pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH)));

                    // Update Kafka with dynamically changeable option that is not controller relevant => controller pod annotations should not change
                    Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME)
                            .edit(k -> new KafkaBuilder(k).editSpec().editKafka().addToConfig(Map.of("compression.type", "gzip")).endKafka().endSpec().build());
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    StrimziPodSet spsControllers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controllers").get();
                    assertThat(spsControllers, is(notNullValue()));

                    spsControllers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                        // Controller annotations be the same
                        assertThat(pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH), is(brokerConfigurationAnnotations.get(pod.getMetadata().getName())));
                    });

                    StrimziPodSet spsBrokers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers").get();
                    assertThat(spsBrokers, is(notNullValue()));

                    spsBrokers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                        // Broker annotations should be the same
                        assertThat(pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH), is(brokerConfigurationAnnotations.get(pod.getMetadata().getName())));
                    });

                    // Update Kafka with dynamically changeable controller relevant option => controller pod annotations should change
                    Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME)
                            .edit(k -> new KafkaBuilder(k).editSpec().editKafka().addToConfig(Map.of("max.connections", "1000")).endKafka().endSpec().build());
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    StrimziPodSet spsControllers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-controllers").get();
                    assertThat(spsControllers, is(notNullValue()));

                    spsControllers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                        // Controller annotations should differ
                        assertThat(pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH), is(not(brokerConfigurationAnnotations.get(pod.getMetadata().getName()))));
                    });

                    StrimziPodSet spsBrokers = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-brokers").get();
                    assertThat(spsBrokers, is(notNullValue()));

                    spsBrokers.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
                        // Broker annotations should be the same
                        assertThat(pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH), is(brokerConfigurationAnnotations.get(pod.getMetadata().getName())));
                    });

                    async.flag();
                })));
    }
}
