/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
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
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.PodRevision;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class PartialRollingUpdateMockTest {
    private static final Logger LOGGER = LogManager.getLogger(PartialRollingUpdateMockTest.class);

    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private KafkaAssemblyOperator kco;
    private StrimziPodSetController podSetController;
    private ResourceOperatorSupplier supplier;

    @BeforeAll
    public static void before() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaNodePoolCrd()
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
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo, VertxTestContext context) throws InterruptedException {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        KafkaNodePool controllers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("controllers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-199]"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();
        Crds.kafkaNodePoolOperation(client).resource(controllers).create();

        KafkaNodePool brokers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("brokers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[0-99]"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();
        Crds.kafkaNodePoolOperation(client).resource(brokers).create();

        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        Crds.kafkaOperation(client).resource(cluster).create();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        supplier = supplier(client, pfa);

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        kco = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        LOGGER.info("Initial reconciliation");
        CountDownLatch createAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("initialization", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            createAsync.countDown();
        });

        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Initial reconciliation timed-out"));
        }

        LOGGER.info("Initial reconciliation complete");

        context.completeNow();
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    ResourceOperatorSupplier supplier(KubernetesClient bootstrapClient, PlatformFeaturesAvailability pfa) {
        return new ResourceOperatorSupplier(vertx,
                bootstrapClient,
                ResourceUtils.adminClientProvider(), ResourceUtils.kafkaAgentClientProvider(),
                ResourceUtils.metricsProvider(),
                pfa
        );
    }

    private void updatePodAnnotation(String podName, String annotationKey, String annotationValue)  {
        client.pods()
                .inNamespace(namespace)
                .withName(podName)
                .edit(pod -> new PodBuilder(pod)
                        .editMetadata()
                            .addToAnnotations(annotationKey, annotationValue)
                        .endMetadata()
                        .build());
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaCluster(VertxTestContext context) {
        // Fake the test state in MockKube
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "controllers", 100), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "brokers", 1), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");

        // Test the next reconciliation fixes it
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<StrimziPodSet> podSets = supplier.strimziPodSetOperator.client().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems();
                    List<Pod> desiredPods = podSets.stream().map(PodSetUtils::podSetToPods).flatMap(List::stream).toList();
                    List<Pod> actualPods = client.pods().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems();

                    assertThat(desiredPods.size(), is(6));
                    assertThat(actualPods.size(), is(6));

                    for (Pod actualPod : actualPods) {
                        String actualRevision = actualPod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION);
                        String desiredRevision = desiredPods
                                .stream()
                                .filter(p -> actualPod.getMetadata().getName().equals(p.getMetadata().getName()))
                                .findFirst()
                                .orElseThrow()
                                .getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION);

                        assertThat(desiredRevision, is(actualRevision));
                    }

                    async.flag();
                })));
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaClusterForServerCertificates(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        // Check the initial state first
        Map<String, Secret> certSecrets = client.secrets().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems()
                .stream().collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));
        context.verify(() -> assertThat(certSecrets.size(), is(6)));
        List<Pod> initialPods = client.pods().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems();
        context.verify(() -> assertThat(initialPods.size(), is(6)));

        for (Pod pod : initialPods) {
            String podCertHash = pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
            String expectedCertHash = CertUtils.getCertificateThumbprint(certSecrets.get(pod.getMetadata().getName()), Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()));
            context.verify(() -> assertThat(podCertHash, is(expectedCertHash)));
        }

        // Fake the test state in MockKube
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "controllers", 102), Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "oldhash");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "brokers", 1), Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "oldhash");

        // Test the next reconciliation fixes it
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<Pod> updatedPods = client.pods().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems();
                    assertThat(updatedPods.size(), is(6));

                    for (Pod pod : initialPods) {
                        String podCertHash = pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
                        String expectedCertHash = CertUtils.getCertificateThumbprint(certSecrets.get(pod.getMetadata().getName()), Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()));
                        assertThat(podCertHash, is(expectedCertHash));
                    }

                    async.flag();
                })));
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClusterCaCertificate(VertxTestContext context) {
        // Fake the test state in MockKube
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "controllers", 100), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "controllers", 100), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "brokers", 1), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "brokers", 1), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");

        // Test the next reconciliation fixes it
        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<Pod> updatedPods = client.pods().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems();
                    assertThat(updatedPods.size(), is(6));

                    for (Pod pod : updatedPods) {
                        String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                        assertThat(certGeneration, is("0"));
                    }

                    async.flag();
                })));
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClientsCaCertificate(VertxTestContext context) {
        // Fake the test state in MockKube
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "controllers", 100), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "controllers", 100), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "brokers", 1), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, "brokers", 1), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "-1");

        // Test the next reconciliation fixes it
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    List<Pod> updatedPods = client.pods().inNamespace(namespace).withLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER_NAME))).list().getItems();
                    assertThat(updatedPods.size(), is(6));

                    for (Pod pod : updatedPods) {
                        String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION);
                        assertThat(certGeneration, is("0"));
                    }
                    async.flag();
                })));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
