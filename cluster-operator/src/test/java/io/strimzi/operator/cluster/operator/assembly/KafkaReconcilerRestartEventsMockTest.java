/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
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
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.ResourceUtils.dummyClusterOperatorConfig;
import static io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator.ANNO_STRIMZI_IO_GENERATION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KafkaReconcilerRestartEventsMockTest {
    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "testkafka";
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.V1_22);
    private final static KafkaVersionChange VERSION_CHANGE = createVersionChange();
    private final static Kafka KAFKA = createKafkaCR();

    private final MockCertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    private final ClusterCa clusterCa = createClusterCa();
    private final ClientsCa clientsCa = createClientsCa();


    private ResourceOperatorSupplier supplier;

    private Reconciliation reconciliation;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;

    private MockKube2 mockKube;

    // Have to use statefulsets for everythinf due to a bug in the mock server that prevents the
    // strimzi pod sets ever coming ready: https://github.com/fabric8io/kubernetes-client/issues/4139
    private final ClusterOperatorConfig useStsForNowConf = dummyClusterOperatorConfig("-UseStrimziPodSets");

    @BeforeEach
    void setup(Vertx vertx) throws ExecutionException, InterruptedException {
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
                mock(ZookeeperLeaderFinder.class),
                ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(),
                PFA,
                60_000);

        // Initial reconciliation to create cluster
        KafkaAssemblyOperator kao = new KafkaAssemblyOperator(vertx, PFA, mockCertManager, passwordGenerator, supplier, useStsForNowConf);
        kao.reconcile(new Reconciliation("initial", "kafka", NAMESPACE, CLUSTER_NAME)).toCompletionStage().toCompletableFuture().get();

        reconciliation = new Reconciliation("test", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    }

    @AfterEach
    void teardown() {
        mockKube.stop();
    }

    // This test uses mocks and an overridden reconcile() method instead of Mock Kube due to a bug in the mock server.
    // It prevents the strimzi pod sets ever coming ready: https://github.com/fabric8io/kubernetes-client/issues/4139
    @Test
    void testEventEmittedWhenPodInPodSetHasOldRevision(Vertx vertx, VertxTestContext context) {
        ResourceOperatorSupplier mockSupplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = mockSupplier.secretOperations;
        ServiceOperator serviceOps = mockSupplier.serviceOperations;
        PodOperator podOps = mockSupplier.podOperations;
        StrimziPodSetOperator strimziPodSetOps = mockSupplier.strimziPodSetOperator;
        KubernetesRestartEventPublisher eventPublisher = mockSupplier.restartEventsPublisher;

        Secret secret = new Secret();
        when(secretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(podOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(podOps.restart(any(), any(), anyLong())).thenReturn(Future.succeededFuture());
        when(podOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(serviceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(serviceOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(strimziPodSetOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));

        // Grab the first pod created by the SPS, so when it filters on name when checking revisions, it finds what it's expecting
        AtomicReference<Pod> firstPod = new AtomicReference<>();
        when(strimziPodSetOps.reconcile(any(), anyString(), anyString(), any(StrimziPodSet.class)))
                .thenAnswer((Answer<Future<ReconcileResult<StrimziPodSet>>>) invocation -> {
                    StrimziPodSet sps = invocation.getArgument(invocation.getArguments().length - 1, StrimziPodSet.class);
                    firstPod.set(PodSetUtils.mapToPod(sps.getSpec().getPods().get(0)));
                    return Future.succeededFuture(ReconcileResult.noop(sps));
                });

        //Update the copy of the first pod to have a revision annotation that doesn't match
        when(podOps.get(any(), anyString())).thenAnswer(inv -> {
            Pod pod = firstPod.get();
            ObjectMeta existingMeta = pod.getMetadata();
            pod.setMetadata(new ObjectMetaBuilder(existingMeta)
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "x"))
                    .build());
            return pod;
        });

        // As event dispatching is happening in a background thread, we need to capture when it's actually happened, otherwise
        // the test finishes before the background thread completes
        AtomicBoolean eventDispatched = new AtomicBoolean(false);
        doAnswer(invocation -> {
            eventDispatched.set(true);
            return null;
        }).when(eventPublisher).publishRestartEvents(any(), any());

        KafkaReconciler reconciler = new KafkaReconciler(reconciliation, KAFKA, null, 1, clusterCa, clientsCa, VERSION_CHANGE, dummyClusterOperatorConfig(), mockSupplier, PFA, vertx) {
            @Override
            public Future<Void> reconcile(KafkaStatus kafkaStatus, Supplier<Date> dateSupplier) {
                //Run subset of reconciliation to avoid mocking the world in the absence of MockKube
                listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
                return podSet().onComplete(i -> rollingUpdate());
            }
        };

        ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
        ArgumentCaptor<RestartReasons> reasonsCaptor = ArgumentCaptor.forClass(RestartReasons.class);

        reconciler.reconcile(new KafkaStatus(), Date::new).onComplete(context.succeeding(i -> context.verify(() -> {
            TestUtils.waitFor("Event publication in worker thread", 500, 10000, eventDispatched::get);

            verify(eventPublisher, times(1)).publishRestartEvents(podCaptor.capture(), reasonsCaptor.capture());

            assertThat(podCaptor.getValue().getMetadata().getName(), is(firstPod.get().getMetadata().getName()));
            assertThat(reasonsCaptor.getValue(), is(RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION)));
            context.completeNow();
        })));
    }

    @Test
    public void testEventEmittedWhenPodInStatefulSetHasOldGeneration(Vertx vertx, VertxTestContext context) {
        KafkaReconciler reconciler = new KafkaReconciler(reconciliation, KAFKA, null, 1, clusterCa, clientsCa, VERSION_CHANGE, useStsForNowConf, supplier, PFA, vertx);

        // Grab STS generation
        StatefulSet kafkaSet = stsOps().withLabel("app.kubernetes.io/name", "kafka").list().getItems().get(0);
        int statefulSetGen = StatefulSetOperator.getStsGeneration(kafkaSet);

        // And set pod generation to be lower than it to trigger reconciliation
        Pod kafkaPod = podOps().withLabel("app.kubernetes.io/name", "kafka").list().getItems().get(0);
        Map<String, String> podAnnotationsCopy = new HashMap<>(kafkaPod.getMetadata().getAnnotations());
        podAnnotationsCopy.put(ANNO_STRIMZI_IO_GENERATION, String.valueOf(statefulSetGen - 1));
        kafkaPod.getMetadata().setAnnotations(podAnnotationsCopy);
        podOps().withName(kafkaPod.getMetadata().getName()).patch(kafkaPod);

        reconciler.reconcile(new KafkaStatus(), Date::new)
                  .onComplete(context.succeeding(i -> context.verify(() -> {
                      TestUtils.waitFor("Event publication in worker thread", 500, 10000, () -> !listEvents().isEmpty());

                      List<Event> events = listEvents();
                      assertThat(events.size(), is(1));
                      Event restartEvent = events.get(0);

                      assertThat(restartEvent.getRegarding().getName(), is(kafkaPod.getMetadata().getName()));
                      assertThat(restartEvent.getReason(), is(RestartReason.POD_HAS_OLD_GENERATION.pascalCased()));
                      context.completeNow();
                  })));
    }

    private NonNamespaceOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> stsOps() {
        return client.apps().statefulSets().inNamespace(NAMESPACE);
    }


    private List<Event> listEvents() {
        EventList list = client.events().v1().events().inNamespace(NAMESPACE).list();
        return list.getItems()
                   .stream()
                   .filter(e -> e.getAction().equals("StrimziInitiatedPodRestart"))
                   .collect(Collectors.toList());
    }

    private NonNamespaceOperation<Pod, PodList, PodResource<Pod>> podOps() {
        return client.pods().inNamespace(NAMESPACE);
    }

    private static KafkaVersionChange createVersionChange() {
        return new KafkaVersionChange(
                VERSIONS.defaultVersion(),
                VERSIONS.defaultVersion(),
                VERSIONS.defaultVersion().protocolVersion(),
                VERSIONS.defaultVersion().messageVersion()
        );
    }

    private ClientsCa createClientsCa() {
        return new ClientsCa(
                Reconciliation.DUMMY_RECONCILIATION,
                mockCertManager,
                passwordGenerator,
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()),
                365,
                30,
                true,
                null
        );
    }

    private ClusterCa createClusterCa() {
        return new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                mockCertManager,
                passwordGenerator,
                CLUSTER_NAME,
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
        );
    }

    private static Kafka createKafkaCR() {
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
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
    }
}
