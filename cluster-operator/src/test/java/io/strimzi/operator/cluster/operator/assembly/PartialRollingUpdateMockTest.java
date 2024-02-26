/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    private Kafka cluster;
    private StrimziPodSetController podSetController;
    private ResourceOperatorSupplier supplier;

    @BeforeAll
    public static void before() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
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
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo, VertxTestContext context) throws InterruptedException {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        cluster = new KafkaBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(CLUSTER_NAME)
                .withNamespace(namespace)
                .build())
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(5)
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
        cluster = Crds.kafkaOperation(client).resource(cluster).create();

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
            context.failNow(new Throwable("Test timeout"));
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
                ResourceUtils.zookeeperLeaderFinder(vertx, bootstrapClient),
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(), ResourceUtils.kafkaAgentClientProvider(),
                ResourceUtils.metricsProvider(),
                pfa,
                60_000L);
    }

    private void updatePodAnnotation(String podName, String annotation, String generation)  {
        client.pods()
                .inNamespace(namespace)
                .withName(podName)
                .edit(pod -> new PodBuilder(pod)
                        .editMetadata()
                            .addToAnnotations(annotation, generation)
                        .endMetadata()
                        .build());
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaCluster(VertxTestContext context) {
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));

            StrimziPodSet kafkaPodSet = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(KafkaResources.kafkaComponentName(CLUSTER_NAME)).get();
            List<Pod> kafkaPodsFromPodSet = PodSetUtils.podSetToPods(kafkaPodSet);

            for (int i = 0; i <= 4; i++) {
                int finalI = i;

                Pod pod = client.pods().inNamespace(namespace).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, i)).get();
                String podRevision = pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION);
                String spsRevision = kafkaPodsFromPodSet
                        .stream()
                        .filter(p -> KafkaResources.kafkaPodName(CLUSTER_NAME, finalI).equals(p.getMetadata().getName()))
                        .findFirst()
                        .orElseThrow()
                        .getMetadata()
                        .getAnnotations()
                        .get(PodRevision.STRIMZI_REVISION_ANNOTATION);

                context.verify(() -> assertThat("Pod " + finalI + " had unexpected revision", podRevision, is(spsRevision)));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaClusterForServerCertificates(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        var brokersSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();

        for (int brokerId = 0; brokerId < cluster.getSpec().getKafka().getReplicas(); brokerId++) {
            var pod = client.pods().inNamespace(namespace).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, brokerId)).get();
            var podCertHash = pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
            var expectedCertHash = CertUtils.getCertificateThumbprint(brokersSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()));

            assertThat("Pod " + brokerId + " had unexpected revision", podCertHash, is(expectedCertHash));
        }

        LOGGER.info("Recovery reconciliation");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 1), Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "oldhash");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "oldhash");

        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));

            for (int brokerId = 0; brokerId < cluster.getSpec().getKafka().getReplicas(); brokerId++) {
                final var finalBrokerId = brokerId;
                var pod = client.pods().inNamespace(namespace).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, brokerId)).get();
                var podCertHash = pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
                var expectedCertHash = CertUtils.getCertificateThumbprint(brokersSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()));

                context.verify(() -> assertThat("Pod " + finalBrokerId + " had unexpected revision", podCertHash, is(expectedCertHash)));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledZookeeperCluster(VertxTestContext context) {
        updatePodAnnotation(KafkaResources.zookeeperPodName(CLUSTER_NAME, 1), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.zookeeperPodName(CLUSTER_NAME, 2), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));

            StrimziPodSet zooPodSet = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(KafkaResources.zookeeperComponentName(CLUSTER_NAME)).get();
            List<Pod> zooPodsFromPodSet = PodSetUtils.podSetToPods(zooPodSet);

            for (int i = 0; i <= 2; i++) {
                int finalI = i;

                Pod pod = client.pods().inNamespace(namespace).withName(KafkaResources.zookeeperPodName(CLUSTER_NAME, i)).get();
                String podRevision = pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION);
                String spsRevision = zooPodsFromPodSet
                        .stream()
                        .filter(p -> KafkaResources.zookeeperPodName(CLUSTER_NAME, finalI).equals(p.getMetadata().getName()))
                        .findFirst()
                        .orElseThrow()
                        .getMetadata()
                        .getAnnotations()
                        .get(PodRevision.STRIMZI_REVISION_ANNOTATION);

                context.verify(() -> assertThat("Pod " + finalI + " had unexpected revision", podRevision, is(spsRevision)));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledZooClusterForServerCerts(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        var zkSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.zookeeperSecretName(CLUSTER_NAME)).get();
        for (int zkIndex = 0; zkIndex < cluster.getSpec().getZookeeper().getReplicas(); zkIndex++) {
            var pod = client.pods().inNamespace(namespace).withName(KafkaResources.zookeeperPodName(CLUSTER_NAME, zkIndex)).get();
            var podCertHash = pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
            var expectedCertHash = CertUtils.getCertificateThumbprint(zkSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()));

            assertThat("Pod " + zkIndex + " had unexpected revision", podCertHash, is(expectedCertHash));
        }

        LOGGER.info("Recovery reconciliation");
        updatePodAnnotation(KafkaResources.zookeeperPodName(CLUSTER_NAME, 1), Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "oldhash");
        updatePodAnnotation(KafkaResources.zookeeperPodName(CLUSTER_NAME, 2), Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "oldhash");

        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));

            for (int zkIndex = 0; zkIndex < cluster.getSpec().getZookeeper().getReplicas(); zkIndex++) {
                final var finalZkIndex = zkIndex;
                var pod = client.pods().inNamespace(namespace).withName(KafkaResources.zookeeperPodName(CLUSTER_NAME, zkIndex)).get();
                var podCertHash = pod.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH);
                var expectedCertHash = CertUtils.getCertificateThumbprint(zkSecret, Ca.SecretEntry.CRT.asKey(pod.getMetadata().getName()));

                context.verify(() -> assertThat("Pod " + finalZkIndex + " had unexpected revision", podCertHash, is(expectedCertHash)));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClusterCaCertificate(VertxTestContext context) {
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.zookeeperPodName(CLUSTER_NAME, 2), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.zookeeperPodName(CLUSTER_NAME, 2), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (int i = 0; i <= 2; i++) {
                Pod pod = client.pods().inNamespace(namespace).withName(KafkaResources.zookeeperPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected cert generation " + certGeneration, certGeneration, is("0")));
            }
            for (int i = 0; i <= 4; i++) {
                Pod pod = client.pods().inNamespace(namespace).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected cert generation " + certGeneration, certGeneration, is("0")));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClientsCaCertificate(VertxTestContext context) {
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "-1");
        updatePodAnnotation(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), PodRevision.STRIMZI_REVISION_ANNOTATION, "notmatchingrevision");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (int i = 0; i <= 4; i++) {
                Pod pod = client.pods().inNamespace(namespace).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected cert generation " + certGeneration, certGeneration, is("0")));
            }
            async.flag();
        });
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
