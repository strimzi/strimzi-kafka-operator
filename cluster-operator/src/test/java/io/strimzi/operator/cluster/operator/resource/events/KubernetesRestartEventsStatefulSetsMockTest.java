/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.events;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.api.model.events.v1.EventList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaReconciler;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.AbstractScalableNamespacedResourceOperator;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.ResourceUtils.createInitialCaCertSecret;
import static io.strimzi.operator.cluster.ResourceUtils.createInitialCaKeySecret;
import static io.strimzi.operator.cluster.ResourceUtils.dummyClusterOperatorConfig;
import static io.strimzi.operator.cluster.model.AbstractModel.clusterCaCertSecretName;
import static io.strimzi.operator.cluster.model.AbstractModel.clusterCaKeySecretName;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS;
import static io.strimzi.operator.cluster.model.RestartReason.CUSTOM_LISTENER_CA_CERT_CHANGE;
import static io.strimzi.operator.cluster.model.RestartReason.JBOD_VOLUMES_CHANGED;
import static io.strimzi.operator.cluster.model.RestartReason.MANUAL_ROLLING_UPDATE;
import static io.strimzi.operator.cluster.model.RestartReason.POD_HAS_OLD_GENERATION;
import static io.strimzi.operator.cluster.model.RestartReason.POD_STUCK;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE;
import static io.strimzi.operator.common.operator.resource.AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_GENERATION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KubernetesRestartEventsStatefulSetsMockTest {

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

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;

    private final ClusterOperatorConfig useStatefulSetsConfig = dummyClusterOperatorConfig("-UseStrimziPodSets");

    private KafkaStatus ks;
    private WorkerExecutor sharedWorkerExecutor;

    @BeforeEach
    void setup(Vertx vertx) throws ExecutionException, InterruptedException {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        mockKube = new MockKube2.MockKube2Builder(client)
                .withMockWebServerLoggingSettings(Level.WARNING, true)
                .withKafkaCrd()
                .withInitialKafkas(KAFKA)
                .withStrimziPodSetCrd()
                .withStatefulSetController()
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

        // Initial reconciliation to create cluster
        KafkaAssemblyOperator kao = new KafkaAssemblyOperator(vertx, PFA, mockCertManager, passwordGenerator, supplier, useStatefulSetsConfig);
        kao.reconcile(new Reconciliation("initial", "kafka", NAMESPACE, CLUSTER_NAME)).toCompletionStage().toCompletableFuture().get();

        reconciliation = new Reconciliation("test", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
        ks = new KafkaStatus();
    }

    @AfterEach
    void teardown() {
        sharedWorkerExecutor.close();
        mockKube.stop();
    }

    @Test
    void testEventEmittedWhenPodInStatefulSetHasOldGeneration(Vertx vertx, VertxTestContext context) {
        KafkaReconciler reconciler = new KafkaReconciler(
                reconciliation,
                KAFKA,
                null,
                1,
                clusterCa,
                clientsCa,
                VERSION_CHANGE,
                useStatefulSetsConfig,
                supplier,
                PFA,
                vertx);

        // Grab STS generation
        StatefulSet kafkaSet = stsOps().withLabel(appName, "kafka").list().getItems().get(0);
        int statefulSetGen = StatefulSetOperator.getStsGeneration(kafkaSet);

        patchKafkaPodWithAnnotation(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_GENERATION, String.valueOf(statefulSetGen - 1));
        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(POD_HAS_OLD_GENERATION, context));
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
                useStatefulSetsConfig,
                supplier,
                PFA,
                vertx
        );

        lowerVolumes.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(JBOD_VOLUMES_CHANGED, context));
    }

    @Test
    void testEventEmittedWhenAnnotatedForManualRollingUpdate(Vertx vertx, VertxTestContext context) {
        StatefulSet kafkaSet = stsOps().withLabel(appName, "kafka").list().getItems().get(0);
        StatefulSet patchedSet = new StatefulSetBuilder(kafkaSet)
                .editMetadata()
                    .addToAnnotations(ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build();

        stsOps().resource(patchedSet).replace();

        KafkaReconciler reconciler = new KafkaReconciler(
                reconciliation,
                KAFKA,
                null,
                1,
                clusterCa,
                clientsCa,
                VERSION_CHANGE,
                useStatefulSetsConfig,
                supplier,
                PFA,
                vertx);

        reconciler.reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(MANUAL_ROLLING_UPDATE, context));
    }

    @Test
    void testEventEmittedWhenCustomListenerCaCertChanged(Vertx vertx, VertxTestContext context) {
        // Change custom listener cert thumbprint annotation to cause reconciliation requiring restart
        patchKafkaPodWithAnnotation(ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, "1234");

        defaultReconciler(vertx).reconcile(ks, Clock.systemUTC()).onComplete(verifyEventPublished(CUSTOM_LISTENER_CA_CERT_CHANGE, context));
    }

    @Test
    void testEventEmittedWhenPodIsStuck(Vertx vertx, VertxTestContext context) {
        Pod kafkaPod = kafkaPod();

        Pod patch = new PodBuilder(kafkaPod)
                .editOrNewMetadata()
                    // Need to do this as the mock pod controller will otherwise override the Status below
                    .addToAnnotations(MockPodController.ANNO_DO_NOT_SET_READY, "True")
                    // Needs to be old gen / old revision for pod stuck to trigger
                    .addToAnnotations(ANNO_STRIMZI_IO_GENERATION, "-1")
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

    private <T> Handler<AsyncResult<T>> verifyEventPublished(RestartReason expectedReason, VertxTestContext context) {
        return context.succeeding(i -> context.verify(() -> {
            TestUtils.waitFor("Event publication in worker thread", 500, 10000, () -> !listRestartEvents().isEmpty());
            String expectedReasonPascal = expectedReason.pascalCased();

            List<Event> events = listRestartEvents();
            Optional<Event> maybeEvent = events.stream().filter(e -> e.getReason().equals(expectedReasonPascal)).findFirst();

            if (maybeEvent.isEmpty()) {
                List<String> foundEvents = listRestartEvents().stream().map(Event::getReason).collect(Collectors.toList());
                throw new AssertionError("Expected restart event " + expectedReasonPascal + " not found. Found these events: " + foundEvents);
            }

            Event restartEvent = maybeEvent.get();
            assertThat(restartEvent.getRegarding().getName(), is(kafkaPod().getMetadata().getName()));
            context.completeNow();
        }));
    }

    private KafkaReconciler defaultReconciler(Vertx vertx) {
        return new KafkaReconciler(reconciliation, KAFKA, null, 1, clusterCa, clientsCa, VERSION_CHANGE, useStatefulSetsConfig, supplier, PFA, vertx);
    }

    private Pod kafkaPod() {
        return podOps().withLabel(appName, "kafka").list().getItems().get(0);
    }

    private NonNamespaceOperation<Pod, PodList, PodResource> podOps() {
        return client.pods().inNamespace(NAMESPACE);
    }

    private NonNamespaceOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> stsOps() {
        return client.apps().statefulSets().inNamespace(NAMESPACE);
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
        return createClusterCaWithSecret();
    }

    private ClusterCa createClusterCaWithSecret() {
        return new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                mockCertManager,
                passwordGenerator,
                CLUSTER_NAME,
                createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
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
}