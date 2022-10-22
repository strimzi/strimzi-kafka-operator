/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube2.MockKube2;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class PartialStatefulSetRollingUpdateMockTest {

    private static final Logger LOGGER = LogManager.getLogger(PartialStatefulSetRollingUpdateMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private KafkaAssemblyOperator kco;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;

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
    public void beforeEach(VertxTestContext context) throws InterruptedException {
        Kafka cluster = new KafkaBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
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

        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaCrd()
                .withInitialKafkas(cluster)
                .withStrimziPodSetCrd()
                .withDeploymentController()
                .withPodController()
                .withStatefulSetController()
                .withServiceController()
                .build();
        mockKube.start();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        ResourceOperatorSupplier supplier = supplier(client, pfa);

        kco = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS, 2_000, "-UseStrimziPodSets"));

        LOGGER.info("Initial reconciliation");
        CountDownLatch createAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("initialization", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).onComplete(ar -> {
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
        mockKube.stop();
    }

    ResourceOperatorSupplier supplier(KubernetesClient bootstrapClient, PlatformFeaturesAvailability pfa) {
        return new ResourceOperatorSupplier(vertx,
                bootstrapClient,
                ResourceUtils.zookeeperLeaderFinder(vertx, bootstrapClient),
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(),
                pfa,
                60_000L);
    }

    private void updateStatefulSetGeneration(String stsName, String annotation, String generation)  {
        client.apps().statefulSets()
                .inNamespace(NAMESPACE)
                .withName(stsName)
                .edit(sts -> new StatefulSetBuilder(sts)
                        .editSpec()
                            .editTemplate()
                                .editMetadata()
                                    .addToAnnotations(annotation, generation)
                                .endMetadata()
                            .endTemplate()
                        .endSpec()
                        .build());
    }

    private void updatePodGeneration(String podName, String annotation, String generation)  {
        client.pods()
                .inNamespace(NAMESPACE)
                .withName(podName)
                .edit(pod -> new PodBuilder(pod)
                        .editMetadata()
                            .addToAnnotations(annotation, generation)
                        .endMetadata()
                        .build());
    }

    private void updateSecretGeneration(String secretName, String annotation, String generation)  {
        client.secrets()
                .inNamespace(NAMESPACE)
                .withName(secretName)
                .edit(secret -> new SecretBuilder(secret)
                        .editMetadata()
                            .addToAnnotations(annotation, generation)
                        .endMetadata()
                        .build());
    }

    @Test
    public void testReconcileOfPartiallyRolledKafkaCluster(VertxTestContext context) {
        updateStatefulSetGeneration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 1), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "2");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 3), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (int i = 0; i <= 4; i++) {
                Pod pod = client.pods().inNamespace(NAMESPACE).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, i)).get();
                String generation = pod.getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected generation " + generation, generation, is("3")));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledZookeeperCluster(VertxTestContext context) {
        updateStatefulSetGeneration(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        updatePodGeneration(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "3");
        updatePodGeneration(KafkaResources.zookeeperPodName(CLUSTER_NAME, 1), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "2");
        updatePodGeneration(KafkaResources.zookeeperPodName(CLUSTER_NAME, 2), StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).onComplete(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (int i = 0; i <= 2; i++) {
                Pod pod = client.pods().inNamespace(NAMESPACE).withName(KafkaResources.zookeeperPodName(CLUSTER_NAME, i)).get();
                String generation = pod.getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected generation " + generation, generation, is("3")));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClusterCaCertificate(VertxTestContext context) {
        updateSecretGeneration(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 1), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "2");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 3), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1");
        updatePodGeneration(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.zookeeperPodName(CLUSTER_NAME, 1), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "2");
        updatePodGeneration(KafkaResources.zookeeperPodName(CLUSTER_NAME, 2), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).onComplete(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (int i = 0; i <= 2; i++) {
                Pod pod = client.pods().inNamespace(NAMESPACE).withName(KafkaResources.zookeeperPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected cert generation " + certGeneration, certGeneration, is("3")));
            }
            for (int i = 0; i <= 4; i++) {
                Pod pod = client.pods().inNamespace(NAMESPACE).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected cert generation " + certGeneration, certGeneration, is("3")));
            }
            async.flag();
        });
    }

    @Test
    public void testReconcileOfPartiallyRolledClusterForClientsCaCertificate(VertxTestContext context) {
        updateSecretGeneration(KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 1), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "3");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 2), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "2");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 3), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "1");
        updatePodGeneration(KafkaResources.kafkaPodName(CLUSTER_NAME, 4), Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "1");

        LOGGER.info("Recovery reconciliation");
        Checkpoint async = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).onComplete(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (int i = 0; i <= 4; i++) {
                Pod pod = client.pods().inNamespace(NAMESPACE).withName(KafkaResources.kafkaPodName(CLUSTER_NAME, i)).get();
                String certGeneration = pod.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION);
                int finalI = i;
                context.verify(() -> assertThat("Pod " + finalI + " had unexpected cert generation " + certGeneration, certGeneration, is("3")));
            }
            async.flag();
        });
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
