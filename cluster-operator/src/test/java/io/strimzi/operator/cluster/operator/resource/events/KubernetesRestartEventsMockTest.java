/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.api.model.events.v1.EventList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.PodRevision;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.operator.assembly.CaReconciler;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaClusterCreator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMetadataStateManager;
import io.strimzi.operator.cluster.operator.assembly.KafkaReconciler;
import io.strimzi.operator.cluster.operator.assembly.StrimziPodSetController;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube3.MockKube3;
import io.strimzi.test.mockkube3.controllers.MockPodController;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.ResourceUtils.createInitialCaCertSecret;
import static io.strimzi.operator.cluster.ResourceUtils.createInitialCaKeySecret;
import static io.strimzi.operator.cluster.ResourceUtils.dummyClusterOperatorConfig;
import static io.strimzi.operator.cluster.model.AbstractModel.clusterCaCertSecretName;
import static io.strimzi.operator.cluster.model.AbstractModel.clusterCaKeySecretName;
import static io.strimzi.operator.cluster.model.RestartReason.CA_CERT_HAS_OLD_GENERATION;
import static io.strimzi.operator.cluster.model.RestartReason.CA_CERT_REMOVED;
import static io.strimzi.operator.cluster.model.RestartReason.CA_CERT_RENEWED;
import static io.strimzi.operator.cluster.model.RestartReason.CLIENT_CA_CERT_KEY_REPLACED;
import static io.strimzi.operator.cluster.model.RestartReason.CLUSTER_CA_CERT_KEY_REPLACED;
import static io.strimzi.operator.cluster.model.RestartReason.CONFIG_CHANGE_REQUIRES_RESTART;
import static io.strimzi.operator.cluster.model.RestartReason.FILE_SYSTEM_RESIZE_NEEDED;
import static io.strimzi.operator.cluster.model.RestartReason.KAFKA_CERTIFICATES_CHANGED;
import static io.strimzi.operator.cluster.model.RestartReason.MANUAL_ROLLING_UPDATE;
import static io.strimzi.operator.cluster.model.RestartReason.POD_HAS_OLD_REVISION;
import static io.strimzi.operator.cluster.model.RestartReason.POD_STUCK;
import static io.strimzi.operator.cluster.model.RestartReason.POD_UNRESPONSIVE;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
@ExtendWith(VertxExtension.class)
public class KubernetesRestartEventsMockTest {
    private final static String CLUSTER_NAME = "testkafka";

    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private final static KafkaVersionChange VERSION_CHANGE = new KafkaVersionChange(
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion().protocolVersion(),
            VERSIONS.defaultVersion().messageVersion(),
            VERSIONS.defaultVersion().metadataVersion()
    );

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private final MockCertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterCa clusterCa = createClusterCa();
    private final ClientsCa clientsCa = createClientsCa();
    private final String appName = "app.kubernetes.io/name";
    private final ClusterOperatorConfig clusterOperatorConfig = dummyClusterOperatorConfig();

    private String namespace;
    private Kafka kafka;
    private ResourceOperatorSupplier supplier;
    private Reconciliation reconciliation;
    private StrimziPodSetController podSetController;
    private KafkaStatus ks;

    @SuppressWarnings("unused")
    private WorkerExecutor sharedWorkerExecutor;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withStrimziPodSetCrd()
                .withPodController()
                .withServiceController()
                .withDeploymentController()
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
    void beforeEach(TestInfo testInfo, Vertx vertx) throws ExecutionException, InterruptedException {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        kafka = Crds.kafkaOperation(client).resource(kafka()).create();

        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        supplier = new ResourceOperatorSupplier(vertx,
                client,
                ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.kafkaAgentClientProvider(),
                ResourceUtils.metricsProvider(),
                PFA,
                60_000);

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        // Initial reconciliation to create cluster
        KafkaAssemblyOperator kao = new KafkaAssemblyOperator(vertx, PFA, mockCertManager, passwordGenerator, supplier, clusterOperatorConfig);
        kao.reconcile(new Reconciliation("initial", "kafka", namespace, CLUSTER_NAME)).toCompletionStage().toCompletableFuture().get();

        reconciliation = new Reconciliation("test", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME);
        ks = new KafkaStatus();
    }

    @AfterEach
    void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    @Test
    void testEventEmittedWhenJbodVolumeMembershipAltered(Vertx vertx, VertxTestContext context) {
        //Default Kafka CR has two volumes, so drop to 1
        Kafka kafkaWithLessVolumes = new KafkaBuilder(kafka)
                .editSpec()
                    .editKafka()
                        .withNewJbodStorage()
                            .withVolumes(volumeWithId(0))
                        .endJbodStorage()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafkaWithLessVolumes,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler lowerVolumes = new KafkaReconciler(reconciliation,
                kafkaWithLessVolumes,
                null,
                kafkaCluster,
                clusterCa,
                clientsCa,
                clusterOperatorConfig,
                supplier,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafkaWithLessVolumes, clusterOperatorConfig.featureGates().useKRaftEnabled())
        );

        lowerVolumes.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(POD_HAS_OLD_REVISION, context));
    }

    @Test
    void testEventEmittedWhenFileSystemResizeRequested(Vertx vertx, VertxTestContext context) {
        // Pretend the resizing process is underway by adding a condition of FileSystemResizePending
        // This will cause the pod to restart to pick up resized PVC
        PersistentVolumeClaim pvc = pvcOps().withLabel(appName, "kafka").list().getItems().get(0);
        PersistentVolumeClaim patch = new PersistentVolumeClaimBuilder(pvc)
                .editOrNewStatus()
                    .withPhase("Bound")
                    .addNewCondition()
                        .withType("FileSystemResizePending")
                        .withStatus("true")
                    .endCondition()
                .endStatus()
                .build();

        pvcOps().resource(patch).updateStatus();

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(FILE_SYSTEM_RESIZE_NEEDED, context));
    }

    @Test
    void testEventEmittedWhenCaCertHasOldGeneration(Vertx vertx, VertxTestContext context) {
        Secret patched = modifySecretWithAnnotation(clusterCa.caCertSecret(), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");
        ClusterCa oldGenClusterCa = createClusterCaWithSecret(patched);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                oldGenClusterCa,
                clientsCa,
                clusterOperatorConfig,
                supplier,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(CA_CERT_HAS_OLD_GENERATION, context));
    }

    @Test
    void testEventEmittedWhenCaCertRemoved(Vertx vertx, VertxTestContext context) {
        ClusterCa ca = new OverridingClusterCa() {
            @Override
            public boolean certsRemoved() {
                return true;
            }
        };

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                ca,
                clientsCa,
                clusterOperatorConfig,
                supplier,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(CA_CERT_REMOVED, context));
    }

    @Test
    void testEventEmittedWhenCaCertRenewed(Vertx vertx, VertxTestContext context) {
        ClusterCa ca = new OverridingClusterCa() {
            @Override
            public boolean certRenewed() {
                return true;
            }
        };

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                ca,
                clientsCa,
                clusterOperatorConfig,
                supplier,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(CA_CERT_RENEWED, context));
    }

    @Test
    void testEventEmittedWhenClientCaCertKeyReplaced(Vertx vertx, VertxTestContext context) {
        // Turn off cert authority generation to cause reconciliation to roll pods
        Kafka kafkaWithoutClientCaGen = new KafkaBuilder(kafka)
                .editSpec()
                    .editOrNewClientsCa()
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build();

        // Bump ca cert generation to make it look newer than pod knows of
        patchClusterSecretWithAnnotation(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "100000");

        CaReconciler reconciler = new CaReconciler(reconciliation, kafkaWithoutClientCaGen, clusterOperatorConfig, supplier, vertx, mockCertManager, passwordGenerator);
        reconciler.reconcile(Clock.systemUTC()).onComplete(verifyEventPublished(CLIENT_CA_CERT_KEY_REPLACED, context));
    }

    @Test
    void testEventEmittedWhenClusterCaCertKeyReplaced(Vertx vertx, VertxTestContext context) {
        //Turn off cert authority generation to cause reconciliation to roll pods
        Kafka kafkaWithoutClusterCaGen = new KafkaBuilder(kafka)
                .editSpec()
                    .editOrNewClusterCa()
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build();

        // Bump ca cert generation to make it look newer than pod knows of
        patchClusterSecretWithAnnotation(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "100001");

        CaReconciler reconciler = new CaReconciler(reconciliation, kafkaWithoutClusterCaGen, clusterOperatorConfig, supplier, vertx, mockCertManager, passwordGenerator);
        reconciler.reconcile(Clock.systemUTC()).onComplete(verifyEventPublished(CLUSTER_CA_CERT_KEY_REPLACED, context));
    }

    @Test
    void testEventEmittedWhenConfigChangeRequiresRestart(Vertx vertx, VertxTestContext context) {
        // Modify mocked configs call to return a new property to trigger a reconfiguration reconciliation that requires a restart
        Admin adminClient = withChangedBrokerConf(ResourceUtils.adminClientProvider().createAdminClient(null, null, null));
        ResourceOperatorSupplier supplierWithModifiedAdmin = supplierWithAdmin(vertx, () -> adminClient);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                clusterCa,
                clientsCa,
                clusterOperatorConfig,
                supplierWithModifiedAdmin,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(CONFIG_CHANGE_REQUIRES_RESTART, context));
    }

    @Test
    void testEventEmittedWhenPodRevisionChanged(Vertx vertx, VertxTestContext context) {
        // Change custom listener cert thumbprint annotation to cause reconciliation requiring restart
        patchKafkaPodWithAnnotation(PodRevision.STRIMZI_REVISION_ANNOTATION, "doesnotmatchthepodset");

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(POD_HAS_OLD_REVISION, context));
    }

    @Test
    void testEventEmittedWhenPodAnnotatedForManualRollingUpdate(Vertx vertx, VertxTestContext context) {
        patchKafkaPodWithAnnotation(ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(MANUAL_ROLLING_UPDATE, context));
    }

    @Test
    void testEventEmittedWhenSpsAnnotatedForManualRollingUpdate(Vertx vertx, VertxTestContext context) {
        supplier.strimziPodSetOperator
                .client()
                .inNamespace(namespace)
                .withName(CLUSTER_NAME + "-kafka")
                .edit(sps -> new StrimziPodSetBuilder(sps)
                        .editMetadata()
                            .addToAnnotations(ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                        .endMetadata()
                        .build());

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(MANUAL_ROLLING_UPDATE, context));
    }

    @Test
    void testEventEmittedWhenPodIsUnresponsive(Vertx vertx, VertxTestContext context) {
        // Simulate not being able to initiate an initial admin client connection broker at all
        ResourceOperatorSupplier supplierWithModifiedAdmin = supplierWithAdmin(vertx, () -> {
            throw new ConfigException("");
        });

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                clusterCa,
                clientsCa,
                clusterOperatorConfig,
                supplierWithModifiedAdmin,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(POD_UNRESPONSIVE, context));
    }

    @Test
    void testEventEmittedWhenPodIsStuck(Vertx vertx, VertxTestContext context) {
        podOps().withName(CLUSTER_NAME + "-kafka-0").edit(pod -> new PodBuilder(pod)
                .editOrNewMetadata()
                    // Need to do this as the mock pod controller will otherwise override the Status below
                    .addToAnnotations(MockPodController.ANNO_DO_NOT_SET_READY, "True")
                    // Needs to be old gen / old revision for pod stuck to trigger
                    .addToAnnotations(PodRevision.STRIMZI_REVISION_ANNOTATION, "doesnotmatchthepodset")
                .endMetadata()
                .build());

        podOps().withName(CLUSTER_NAME + "-kafka-0").editStatus(pod -> new PodBuilder(pod)
                // Make pod unschedulable
                .editOrNewStatus()
                    .withPhase("Pending")
                    .addNewCondition()
                        .withType("PodScheduled")
                        .withReason("Unschedulable")
                        .withStatus("False")
                    .endCondition()
                .endStatus()
                .build());

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(POD_STUCK, context));
    }

    @Test
    void testEventEmittedWhenKafkaBrokerCertsChanged(Vertx vertx, VertxTestContext context) {
        // Using the real SSL cert manager (after the cluster was created using the mock cert manager) will cause the desired Kafka broker certs to change,
        // thus the reconciliation will schedule the restart needed to pick them up
        ClusterCa changedCa = new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                new OpenSslCertManager(),
                passwordGenerator,
                CLUSTER_NAME,
                createInitialCaCertSecret(namespace, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                createInitialCaKeySecret(namespace, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
        );

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                changedCa,
                clientsCa,
                clusterOperatorConfig,
                supplier,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));
        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(KAFKA_CERTIFICATES_CHANGED, context));

    }

    private <T> Handler<AsyncResult<T>> verifyEventPublished(RestartReason expectedReason, VertxTestContext context) {
        return context.succeeding(i -> context.verify(() -> {
            TestUtils.waitFor("Event publication in worker thread", 500, 10000, () -> !listRestartEvents().isEmpty());
            String expectedReasonPascal = expectedReason.pascalCased();

            List<Event> events = listRestartEvents();
            Optional<Event> maybeEvent = events.stream().filter(e -> e.getReason().equals(expectedReasonPascal)).findFirst();

            if (maybeEvent.isEmpty()) {
                List<String> foundEvents = listRestartEvents().stream().map(Event::getReason).toList();
                throw new AssertionError("Expected restart event " + expectedReasonPascal + " not found. Found these events: " + foundEvents);
            }

            Event restartEvent = maybeEvent.get();
            assertThat(restartEvent.getRegarding().getName(), is(kafkaPod().getMetadata().getName()));
            context.completeNow();
        }));
    }

    private KafkaReconciler defaultReconciler(Vertx vertx) {
        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(reconciliation,
                kafka,
                null,
                Map.of(),
                Map.of(CLUSTER_NAME + "-kafka", List.of(CLUSTER_NAME + "-kafka-0")),
                VERSION_CHANGE,
                KafkaMetadataConfigurationState.ZK,
                VERSIONS,
                supplier.sharedEnvironmentProvider);
        return new KafkaReconciler(reconciliation,
                kafka,
                null,
                kafkaCluster,
                clusterCa,
                clientsCa,
                clusterOperatorConfig,
                supplier,
                PFA,
                vertx,
                new KafkaMetadataStateManager(reconciliation, kafka, clusterOperatorConfig.featureGates().useKRaftEnabled()));
    }

    private ResourceOperatorSupplier supplierWithAdmin(Vertx vertx, Supplier<Admin> adminClientSupplier) {
        AdminClientProvider adminClientProvider = new AdminClientProvider() {
            @Override
            public Admin createAdminClient(String bootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity) {
                return adminClientSupplier.get();
            }

            @Override
            public Admin createAdminClient(String bootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity, Properties config) {
                return adminClientSupplier.get();
            }
        };

        return new ResourceOperatorSupplier(vertx,
                client,
                ResourceUtils.zookeeperLeaderFinder(vertx, client),
                adminClientProvider,
                ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.kafkaAgentClientProvider(),
                ResourceUtils.metricsProvider(),
                PFA,
                60_000);
    }

    private Admin withChangedBrokerConf(Admin preMockedAdminClient) {
        DescribeConfigsResult mockDcr = mock(DescribeConfigsResult.class);

        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        String twoMegAsBytes = Quantity.getAmountInBytes(Quantity.parse("2Mi")).toPlainString();
        KafkaFuture<Config> changedMessageMaxBytes = KafkaFuture.completedFuture(new Config(List.of(new ConfigEntry("message.max.bytes", twoMegAsBytes))));

        when(preMockedAdminClient.describeConfigs(List.of(brokerResource))).thenReturn(mockDcr);
        when(mockDcr.values()).thenReturn(Map.of(brokerResource, changedMessageMaxBytes));
        return preMockedAdminClient;
    }

    private Pod kafkaPod() {
        return podOps().withLabel(appName, "kafka").list().getItems().get(0);
    }

    private NonNamespaceOperation<Pod, PodList, PodResource> podOps() {
        return client.pods().inNamespace(namespace);
    }

    private NonNamespaceOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> pvcOps() {
        return client.persistentVolumeClaims().inNamespace(namespace);
    }

    private List<Event> listRestartEvents() {
        EventList list = client.events().v1().events().inNamespace(namespace).list();
        return list.getItems()
                   .stream()
                   .filter(e -> e.getAction().equals("StrimziInitiatedPodRestart"))
                   .collect(Collectors.toList());
    }

    private void patchKafkaPodWithAnnotation(String annotationName, String annotationValue) {
        Pod kafkaPod = kafkaPod();
        Pod podPatch = new PodBuilder(kafkaPod)
                .editMetadata()
                    .addToAnnotations(annotationName, annotationValue)
                .endMetadata()
                .build();
        podOps().resource(podPatch).update();
    }

    private ClusterCa createClusterCa() {
        return createClusterCaWithSecret(null);
    }

    private ClusterCa createClusterCaWithSecret(Secret caCertSecret) {
        return new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                mockCertManager,
                passwordGenerator,
                CLUSTER_NAME,
                caCertSecret != null ? caCertSecret : createInitialCaCertSecret(namespace, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                createInitialCaKeySecret(namespace, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
        );
    }

    private ClientsCa createClientsCa() {
        return new ClientsCa(
                Reconciliation.DUMMY_RECONCILIATION,
                mockCertManager,
                passwordGenerator,
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                createInitialCaCertSecret(namespace, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                createInitialCaKeySecret(namespace, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()),
                365,
                30,
                true,
                null
        );
    }

    private void patchClusterSecretWithAnnotation(String annotation, String value) {
        Secret brokerSecret = client.secrets().inNamespace(namespace).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();
        Secret patchedSecret = modifySecretWithAnnotation(brokerSecret, annotation, value);

        client.secrets().resource(patchedSecret).update();
    }

    private Secret modifySecretWithAnnotation(Secret brokerSecret, String annotation, String value) {
        return new SecretBuilder(brokerSecret)
                .editMetadata()
                    .addToAnnotations(annotation, value)
                .endMetadata()
                .build();
    }

    private Kafka kafka() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewJbodStorage()
                            .withVolumes(List.of(volumeWithId(0), volumeWithId(1)))
                        .endJbodStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
    }

    private static PersistentClaimStorage volumeWithId(int id) {
        return new PersistentClaimStorageBuilder()
                .withId(id)
                .withDeleteClaim(true)
                .withSize("100Mi")
                .build();
    }

    class OverridingClusterCa extends ClusterCa {
        OverridingClusterCa() {
            super(Reconciliation.DUMMY_RECONCILIATION,
                    mockCertManager,
                    passwordGenerator,
                    CLUSTER_NAME,
                    createInitialCaCertSecret(namespace, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                    createInitialCaKeySecret(namespace, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()));
        }
    }
}