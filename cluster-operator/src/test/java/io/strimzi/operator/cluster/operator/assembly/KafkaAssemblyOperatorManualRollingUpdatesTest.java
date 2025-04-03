/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorManualRollingUpdatesTest {
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER_NAME = "my-cluster";

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withAnnotations(Map.of(
                        Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled",
                        Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"
                ))
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build();
    private static final KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[0-9]"))
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private static final KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[10-99]"))
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    private static Vertx vertx;

    @SuppressWarnings("unused")
    private static WorkerExecutor sharedWorkerExecutor;

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

    @Test
    public void testNoManualRollingUpdateWithPodSets(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSets(false, null, null, node -> null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(KAFKA));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                reconciliation,
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kr.maybeRollKafkaInvocations, is(0));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(0));

                    async.flag();
                })));
    }

    @Test
    public void testManualRollingUpdateWithPodSets(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            List<StrimziPodSet> podSets = kafkaCluster.generatePodSets(false, null, null, node -> null);
            podSets.stream()
                    .filter(ps -> (CLUSTER_NAME + "-brokers").equals(ps.getMetadata().getName()))
                    .findFirst()
                    .orElseThrow()
                    .getMetadata()
                    .getAnnotations()
                    .put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return Future.succeededFuture(podSets);
        });

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(KAFKA));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                reconciliation,
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Kafka rolling updates
                    assertThat(kr.maybeRollKafkaInvocations, is(1));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(3));
                    assertThat(kr.kafkaNodesNeedRestart, is(List.of("my-cluster-brokers-10", "my-cluster-brokers-11", "my-cluster-brokers-12")));
                    assertThat(kr.kafkaRestartReasons.apply(podWithName("anyName")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
    }

    @Test
    public void testManualPodRollingUpdateWithPodSets(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSets(false, null, null, node -> null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithNameAndAnnotations("my-cluster-controllers-0", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-controllers-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithName("my-cluster-controllers-2"));
            pods.add(podWithNameAndAnnotations("my-cluster-brokers-10", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-brokers-11", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithName("my-cluster-brokers-12"));

            return Future.succeededFuture(pods);
        });

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(KAFKA));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                reconciliation,
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Kafka rolling updates
                    assertThat(kr.maybeRollKafkaInvocations, is(1));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(4));
                    assertThat(kr.kafkaNodesNeedRestart, is(List.of("my-cluster-controllers-0", "my-cluster-controllers-1", "my-cluster-brokers-10", "my-cluster-brokers-11")));
                    assertThat(kr.kafkaRestartReasons.apply(podWithName("anyName")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
    }

    @Test
    public void testManualPodRollingUpdateWithPodSetsWithError1(VertxTestContext context) {
        testManualPodRollingUpdateWithPodSetsWithErrorConditions(context, "", true);
    }

    @Test
    public void testManualPodRollingUpdateWithPodSetsWithError3(VertxTestContext context) {
        testManualPodRollingUpdateWithPodSetsWithErrorConditions(context, "", false);
    }

    private void testManualPodRollingUpdateWithPodSetsWithErrorConditions(VertxTestContext context, String featureGates, boolean expectError) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSets(false, null, null, node -> null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithNameAndAnnotations("my-cluster-controllers-0", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-controllers-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithName("my-cluster-controllers-2"));
            pods.add(podWithNameAndAnnotations("my-cluster-brokers-10", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-brokers-11", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithName("my-cluster-brokers-12"));

            return Future.succeededFuture(pods);
        });

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(KAFKA));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(KAFKA);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(featureGates);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                reconciliation,
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                KAFKA,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                kafkaCluster,
                null,
                null,
                true); // => Tells the mock to produce a failure

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                kr);

        Checkpoint async = context.checkpoint();
        if (expectError) {
            kao.reconcile(reconciliation)
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e.getMessage(), containsString("Force failure"));
                    async.flag();
                })));
        } else {
            kao.reconcile(reconciliation)
                .onComplete(context.succeeding(e -> context.verify(() -> {
                    // Verify Kafka rolling updates
                    assertThat(kr.maybeRollKafkaInvocations, is(1));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(4));
                    assertThat(kr.kafkaNodesNeedRestart, is(List.of("my-cluster-controllers-0", "my-cluster-controllers-1", "my-cluster-brokers-10", "my-cluster-brokers-11")));
                    assertThat(kr.kafkaRestartReasons.apply(podWithName("anyName")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
        }
    }

    // Internal utility methods
    private Pod podWithName(String name) {
        return podWithNameAndAnnotations(name, Collections.emptyMap());
    }

    private Pod podWithNameAndAnnotations(String name, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .endMetadata()
                .build();
    }

    static class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        KafkaReconciler mockKafkaReconciler;

        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config, KafkaReconciler mockKafkaReconciler) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
            this.mockKafkaReconciler = mockKafkaReconciler;
        }

        ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            return new MockReconciliationState(reconciliation, kafkaAssembly);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return Future.succeededFuture(reconcileState)
                    .compose(state -> state.reconcileKafka(this.clock))
                    .mapEmpty();
        }

        class MockReconciliationState extends KafkaAssemblyOperator.ReconciliationState {
            MockReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
                super(reconciliation, kafkaAssembly);
            }

            @Override
            Future<KafkaReconciler> kafkaReconciler()    {
                return Future.succeededFuture(mockKafkaReconciler);
            }
        }
    }

    static class MockKafkaReconciler extends KafkaReconciler   {
        int maybeRollKafkaInvocations = 0;
        Function<Pod, RestartReasons> kafkaRestartReasons = null;
        List<String> kafkaNodesNeedRestart = new ArrayList<>();
        private final boolean forceErrorWhenRollKafka;
        public MockKafkaReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, List<KafkaNodePool> nodePools, KafkaCluster kafkaCluster, ClusterCa clusterCa, ClientsCa clientsCa, boolean forceErrorWhenRollKafka) {
            super(reconciliation, kafkaAssembly, nodePools, kafkaCluster, clusterCa, clientsCa, config, supplier, pfa, vertx);
            this.forceErrorWhenRollKafka = forceErrorWhenRollKafka;
        }

        public MockKafkaReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, List<KafkaNodePool> nodePools, KafkaCluster kafkaCluster, ClusterCa clusterCa, ClientsCa clientsCa) {
            this(reconciliation, vertx, config, supplier, pfa, kafkaAssembly, nodePools, kafkaCluster, clusterCa, clientsCa, false);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return manualRollingUpdate();
        }

        @Override
        protected Future<Void> maybeRollKafka(
                Set<NodeRef> nodes,
                Function<Pod, RestartReasons> podNeedsRestart,
                Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
                Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
                boolean allowReconfiguration
        ) {
            maybeRollKafkaInvocations++;
            kafkaRestartReasons = podNeedsRestart;
            kafkaNodesNeedRestart.addAll(nodes.stream().map(NodeRef::podName).toList());
            if (forceErrorWhenRollKafka) {
                return Future.failedFuture("Force failure");
            }
            return Future.succeededFuture();
        }
    }
}
