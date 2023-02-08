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
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.operator.assembly.CaReconciler;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaReconciler;
import io.strimzi.operator.cluster.operator.assembly.StrimziPodSetController;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube2.MockKube2;
import io.strimzi.test.mockkube2.controllers.MockPodController;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.logging.Level;
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
@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KubernetesRestartEventsMockTest {

    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "testkafka";

    private final static Kafka KAFKA = kafka();
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.V1_22);
    private final static KafkaVersionChange VERSION_CHANGE = new KafkaVersionChange(
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion().protocolVersion(),
            VERSIONS.defaultVersion().messageVersion()
    );

    private final MockCertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterCa clusterCa = createClusterCa();
    private final ClientsCa clientsCa = createClientsCa();
    private final String appName = "app.kubernetes.io/name";

    private ResourceOperatorSupplier supplier;
    private Reconciliation reconciliation;
    private MockKube2 mockKube;
    private StrimziPodSetController podSetController;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;

    private final ClusterOperatorConfig useStrimziPodSetsConfig = dummyClusterOperatorConfig();

    private KafkaStatus ks;

    @SuppressWarnings("unused")
    private WorkerExecutor sharedWorkerExecutor;

    @BeforeEach
    void setup(Vertx vertx) throws ExecutionException, InterruptedException {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        mockKube = new MockKube2.MockKube2Builder(client)
                .withMockWebServerLoggingSettings(Level.WARNING, true)
                .withKafkaCrd()
                .withInitialKafkas(KAFKA)
                .withStrimziPodSetCrd()
                .withPodController()
                .withServiceController()
                .withDeploymentController()
                .build();
        mockKube.start();

        supplier = new ResourceOperatorSupplier(vertx,
                client,
                ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(),
                PFA,
                60_000);

        podSetController = new StrimziPodSetController(NAMESPACE, Labels.EMPTY, supplier.kafkaOperator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, ClusterOperatorConfig.DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
        podSetController.start();

        // Initial reconciliation to create cluster
        KafkaAssemblyOperator kao = new KafkaAssemblyOperator(vertx, PFA, mockCertManager, passwordGenerator, supplier, useStrimziPodSetsConfig);
        kao.reconcile(new Reconciliation("initial", "kafka", NAMESPACE, CLUSTER_NAME)).toCompletionStage().toCompletableFuture().get();

        reconciliation = new Reconciliation("test", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
        ks = new KafkaStatus();
    }

    @AfterEach
    void teardown() {
        podSetController.stop();
        mockKube.stop();
    }

    @Test
    void testEventEmittedWhenJbodVolumeMembershipAltered(Vertx vertx, VertxTestContext context) {
        //Default Kafka CR has two volumes, so drop to 1
        Kafka kafkaWithLessVolumes = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJbodStorage()
                            .withVolumes(volumeWithId(0))
                        .endJbodStorage()
                    .endKafka()
                .endSpec()
                .build();

        KafkaReconciler lowerVolumes = new KafkaReconciler(reconciliation,
                kafkaWithLessVolumes,
                null,
                1,
                clusterCa,
                clientsCa,
                VERSION_CHANGE,
                useStrimziPodSetsConfig,
                supplier,
                PFA,
                vertx
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

        pvcOps().resource(patch).replace();

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(FILE_SYSTEM_RESIZE_NEEDED, context));
    }

    @Test
    void testEventEmittedWhenCaCertHasOldGeneration(Vertx vertx, VertxTestContext context) {
        Secret patched = modifySecretWithAnnotation(clusterCa.caCertSecret(), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "-1");
        ClusterCa oldGenClusterCa = createClusterCaWithSecret(patched);

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                KAFKA,
                null,
                1,
                oldGenClusterCa,
                clientsCa,
                VERSION_CHANGE,
                useStrimziPodSetsConfig,
                supplier,
                PFA,
                vertx);

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

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                KAFKA,
                null,
                1,
                ca,
                clientsCa,
                VERSION_CHANGE,
                useStrimziPodSetsConfig,
                supplier,
                PFA,
                vertx);

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

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                KAFKA,
                null,
                1,
                ca,
                clientsCa,
                VERSION_CHANGE,
                useStrimziPodSetsConfig,
                supplier,
                PFA,
                vertx);

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(CA_CERT_RENEWED, context));
    }

    @Test
    void testEventEmittedWhenClientCaCertKeyReplaced(Vertx vertx, VertxTestContext context) {
        // Turn off cert authority generation to cause reconciliation to roll pods
        Kafka kafkaWithoutClientCaGen = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editOrNewClientsCa()
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build();

        // Bump ca cert generation to make it look newer than pod knows of
        patchClusterSecretWithAnnotation(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "100000");

        CaReconciler reconciler = new CaReconciler(reconciliation, kafkaWithoutClientCaGen, useStrimziPodSetsConfig, supplier, vertx, mockCertManager, passwordGenerator);
        reconciler.reconcile(Clock.systemUTC()).onComplete(verifyEventPublished(CLIENT_CA_CERT_KEY_REPLACED, context));
    }

    @Test
    void testEventEmittedWhenClusterCaCertKeyReplaced(Vertx vertx, VertxTestContext context) {
        //Turn off cert authority generation to cause reconciliation to roll pods
        Kafka kafkaWithoutClusterCaGen = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editOrNewClusterCa()
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build();

        // Bump ca cert generation to make it look newer than pod knows of
        patchClusterSecretWithAnnotation(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "100001");

        CaReconciler reconciler = new CaReconciler(reconciliation, kafkaWithoutClusterCaGen, useStrimziPodSetsConfig, supplier, vertx, mockCertManager, passwordGenerator);
        reconciler.reconcile(Clock.systemUTC()).onComplete(verifyEventPublished(CLUSTER_CA_CERT_KEY_REPLACED, context));
    }

    @Test
    void testEventEmittedWhenConfigChangeRequiresRestart(Vertx vertx, VertxTestContext context) {
        // Modify mccked configs call to return a new property to trigger a reconfiguration reconciliation that requires a restart
        Admin adminClient = withChangedBrokerConf(ResourceUtils.adminClientProvider().createAdminClient(null, null, null, null));
        ResourceOperatorSupplier supplierWithModifiedAdmin = supplierWithAdmin(vertx, () -> adminClient);

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                KAFKA,
                null,
                1,
                clusterCa,
                clientsCa,
                VERSION_CHANGE,
                useStrimziPodSetsConfig,
                supplierWithModifiedAdmin,
                PFA,
                vertx);

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
    void testEventEmittedWhenStrimziPodSetAnnotatedForManualRollingUpdate(Vertx vertx, VertxTestContext context) {
        supplier.strimziPodSetOperator
                .client()
                .inNamespace(NAMESPACE)
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

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation,
                KAFKA,
                null,
                1,
                clusterCa,
                clientsCa,
                VERSION_CHANGE,
                useStrimziPodSetsConfig,
                supplierWithModifiedAdmin,
                PFA,
                vertx);

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(POD_UNRESPONSIVE, context));
    }

    @Test
    void testEventEmittedWhenPodIsStuck(Vertx vertx, VertxTestContext context) {
        Pod kafkaPod = kafkaPod();

        Pod patch = new PodBuilder(kafkaPod)
                .editOrNewMetadata()
                    // Need to do this as the mock pod controller will otherwise override the Status below
                    .addToAnnotations(MockPodController.ANNO_DO_NOT_SET_READY, "True")
                    // Needs to be old gen / old revision for pod stuck to trigger
                    .addToAnnotations(PodRevision.STRIMZI_REVISION_ANNOTATION, "doesnotmatchthepodset")
                .endMetadata()
                // Make pod unschedulable
                .editOrNewStatus()
                    .withPhase("Pending")
                    .addNewCondition()
                        .withType("PodScheduled")
                        .withReason("Unschedulable")
                        .withStatus("False")
                    .endCondition()
                .endStatus()
                .build();

        podOps().resource(patch).replace();

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
                createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
        );

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation, KAFKA, null, 1, changedCa, clientsCa, VERSION_CHANGE, useStrimziPodSetsConfig, supplier, PFA, vertx);
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
        return new KafkaReconciler(reconciliation, KAFKA, null, 1, clusterCa, clientsCa, VERSION_CHANGE, useStrimziPodSetsConfig, supplier, PFA, vertx);
    }

    private ResourceOperatorSupplier supplierWithAdmin(Vertx vertx, Supplier<Admin> adminClientSupplier) {
        AdminClientProvider adminClientProvider = new AdminClientProvider() {
            @Override
            public Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName) {
                return adminClientSupplier.get();
            }

            @Override
            public Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName, Properties config) {
                return adminClientSupplier.get();
            }
        };

        return new ResourceOperatorSupplier(vertx,
                client,
                ResourceUtils.zookeeperLeaderFinder(vertx, client),
                adminClientProvider,
                ResourceUtils.zookeeperScalerProvider(),
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
        return client.pods().inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> pvcOps() {
        return client.persistentVolumeClaims().inNamespace(NAMESPACE);
    }

    private List<Event> listRestartEvents() {
        EventList list = client.events().v1().events().inNamespace(NAMESPACE).list();
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
        podOps().resource(podPatch).replace();
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
                caCertSecret != null ? caCertSecret : createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
        );
    }

    private ClientsCa createClientsCa() {
        return new ClientsCa(
                Reconciliation.DUMMY_RECONCILIATION,
                mockCertManager,
                passwordGenerator,
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()),
                365,
                30,
                true,
                null
        );
    }

    private void patchClusterSecretWithAnnotation(String annotation, String value) {
        Secret brokerSecret = client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.kafkaSecretName(CLUSTER_NAME)).get();
        Secret patchedSecret = modifySecretWithAnnotation(brokerSecret, annotation, value);

        client.secrets().resource(patchedSecret).replace();
    }

    private Secret modifySecretWithAnnotation(Secret brokerSecret, String annotation, String value) {
        return new SecretBuilder(brokerSecret)
                .editMetadata()
                    .addToAnnotations(annotation, value)
                .endMetadata()
                .build();
    }

    private static Kafka kafka() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
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
                    createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                    createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()));
        }
    }
}