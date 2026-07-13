/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerSkipRollingUpdateTest {
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER_NAME = "my-cluster";

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
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

    private static KafkaNodePool controllersPool()  {
        return controllersPool(null);
    }

    private static KafkaNodePool controllersPool(String skipAnnotation)  {
        return pool("controllers", "[0-9]", skipAnnotation, ProcessRoles.CONTROLLER);
    }

    private static KafkaNodePool brokersPool(String skipAnnotation)  {
        return pool("brokers", "[10-99]", skipAnnotation, ProcessRoles.BROKER);
    }

    private static KafkaNodePool pool(String name, String nextNodeIds, String skipAnnotation, ProcessRoles... roles)  {
        KafkaNodePoolBuilder builder = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, nextNodeIds))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(roles)
                .endSpec();

        if (skipAnnotation != null) {
            builder.editMetadata().addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, skipAnnotation).endMetadata();
        }

        return builder.build();
    }

    private MockKafkaReconciler prepareReconciler(ResourceOperatorSupplier supplier, List<KafkaNodePool> nodePools)  {
        return prepareReconciler(supplier, KAFKA, nodePools);
    }

    private MockKafkaReconciler prepareReconciler(ResourceOperatorSupplier supplier, Kafka kafka, List<KafkaNodePool> nodePools)  {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                kafka,
                nodePools,
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenReturn(CompletableFuture.completedFuture(kafkaCluster.generatePodSets(null, null, node -> null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(CompletableFuture.completedFuture(List.of()));
        when(mockPodOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        return new MockKafkaReconciler(
                reconciliation,
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                nodePools,
                kafkaCluster);
    }

    @Test
    public void testSkippedNodesAreExcludedFromRollingUpdateAndPodsReady(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        MockKafkaReconciler kr = prepareReconciler(supplier, List.of(controllersPool(), brokersPool("[10,12]")));

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // The skips were resolved
                    assertThat(kr.rollingUpdateSkips.skippedNodeIds(), is(Set.of(10, 12)));

                    // The automatic rolling update never considered the skipped nodes
                    assertThat(kr.kafkaNodesConsideredForRolling, hasItems("my-cluster-controllers-0", "my-cluster-controllers-1", "my-cluster-controllers-2", "my-cluster-brokers-11"));
                    assertThat(kr.kafkaNodesConsideredForRolling, not(hasItems("my-cluster-brokers-10", "my-cluster-brokers-12")));

                    // The readiness of the skipped Pods was not waited on
                    verify(supplier.podOperations, never()).readiness(any(), eq(NAMESPACE), eq("my-cluster-brokers-10"), anyLong(), anyLong());
                    verify(supplier.podOperations, never()).readiness(any(), eq(NAMESPACE), eq("my-cluster-brokers-12"), anyLong(), anyLong());
                    verify(supplier.podOperations).readiness(any(), eq(NAMESPACE), eq("my-cluster-brokers-11"), anyLong(), anyLong());
                    verify(supplier.podOperations).readiness(any(), eq(NAMESPACE), eq("my-cluster-controllers-0"), anyLong(), anyLong());

                    // The RollingUpdateSkipped condition is set on the Kafka status
                    Condition condition = skippedCondition(status);
                    assertThat(condition, is(notNullValue()));
                    assertThat(condition.getMessage(), containsString("Nodes [10, 12] are excluded from automatic rolling updates"));

                    // The skip enabled event was published
                    verify(supplier.restartEventsPublisher).publishClusterEvent(any(), anyString(), eq("RollingUpdateSkipEnabled"), eq("Warning"), contains("[10, 12]"));

                    async.flag();
                })));
    }

    @Test
    public void testManualRollingUpdateIsNotFiltered(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        List<KafkaNodePool> nodePools = List.of(controllersPool(), brokersPool("[10,12]"));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                KAFKA,
                nodePools,
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        // The manual rolling update annotation is set on the brokers PodSet
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            var podSets = kafkaCluster.generatePodSets(null, null, node -> null);
            podSets.stream()
                    .filter(ps -> (CLUSTER_NAME + "-brokers").equals(ps.getMetadata().getName()))
                    .findFirst()
                    .orElseThrow()
                    .getMetadata()
                    .getAnnotations()
                    .put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return CompletableFuture.completedFuture(podSets);
        });

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(CompletableFuture.completedFuture(List.of()));
        when(mockPodOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                reconciliation,
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                KAFKA,
                nodePools,
                kafkaCluster);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // The manual rolling update includes the skipped nodes => a fresh human action outranks a standing skip
                    assertThat(kr.kafkaNodesConsideredForRolling, hasItems("my-cluster-brokers-10", "my-cluster-brokers-11", "my-cluster-brokers-12"));

                    async.flag();
                })));
    }

    @Test
    public void testControllerSkipIsIgnored(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        MockKafkaReconciler kr = prepareReconciler(supplier, List.of(controllersPool("[0,1]"), brokersPool(null)));

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // No node is skipped => the controllers stay fully managed
                    assertThat(kr.rollingUpdateSkips.hasSkippedNodes(), is(false));
                    assertThat(kr.rollingUpdateSkips.ignoredControllerNodeIds(), is(Set.of(0, 1)));
                    assertThat(kr.kafkaNodesConsideredForRolling, hasItems("my-cluster-controllers-0", "my-cluster-controllers-1", "my-cluster-controllers-2", "my-cluster-brokers-10", "my-cluster-brokers-11", "my-cluster-brokers-12"));

                    // No node is skipped => no RollingUpdateSkipped condition, but a warning reports the ignored IDs
                    assertThat(skippedCondition(status), is(nullValue()));
                    Condition warning = warningCondition(status, "SkipRollingUpdateControllersIgnored");
                    assertThat(warning, is(notNullValue()));
                    assertThat(warning.getMessage(), containsString("[0, 1]"));

                    // No skip event was published as no node entered the skipped state
                    verify(supplier.restartEventsPublisher, never()).publishClusterEvent(any(), anyString(), anyString(), anyString(), anyString());

                    async.flag();
                })));
    }

    @Test
    public void testNoSkipAnnotationLeavesBehaviorUnchanged(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        MockKafkaReconciler kr = prepareReconciler(supplier, List.of(controllersPool(), brokersPool(null)));

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kr.rollingUpdateSkips.hasSkippedNodes(), is(false));
                    assertThat(kr.kafkaNodesConsideredForRolling.size(), is(6));
                    assertThat(skippedCondition(status), is(nullValue()));
                    verify(supplier.restartEventsPublisher, never()).publishClusterEvent(any(), anyString(), anyString(), anyString(), anyString());

                    async.flag();
                })));
    }

    @Test
    public void testChangedSkipSetEmitsChangedEvent(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // The previous reconciliation skipped node 10 only
        Kafka kafkaWithStatus = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .addNewCondition()
                        .withType(RollingUpdateSkips.ROLLING_UPDATE_SKIPPED_CONDITION_TYPE)
                        .withStatus("True")
                        .withMessage("Nodes [10] are excluded from automatic rolling updates through the strimzi.io/skip-rolling-update annotation.")
                    .endCondition()
                .endStatus()
                .build();

        MockKafkaReconciler kr = prepareReconciler(supplier, kafkaWithStatus, List.of(controllersPool(), brokersPool("[10,12]")));

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(supplier.restartEventsPublisher).publishClusterEvent(any(), anyString(), eq("RollingUpdateSkipChanged"), eq("Warning"), contains("[10, 12]"));
                    verify(supplier.restartEventsPublisher, never()).publishClusterEvent(any(), anyString(), eq("RollingUpdateSkipEnabled"), anyString(), anyString());

                    async.flag();
                })));
    }

    @Test
    public void testSkippedNodeSoleInSyncReplicaIsFlagged(VertxTestContext context)  {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Partition my-topic-0 has node 10 (which is skipped) as its only in-sync replica. Partition my-topic-1 is
        // fully in-sync and must not be flagged.
        Node node10 = new Node(10, "broker-10", 9092);
        Node node11 = new Node(11, "broker-11", 9092);
        TopicDescription topicDescription = new TopicDescription("my-topic", false, List.of(
                new TopicPartitionInfo(0, node10, List.of(node10, node11), List.of(node10)),
                new TopicPartitionInfo(1, node11, List.of(node10, node11), List.of(node10, node11))
        ));

        Admin mockAdmin = supplier.adminClientProvider.createAdminClient(null, null, null);
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenReturn(KafkaFuture.completedFuture(Set.of("my-topic")));
        when(mockAdmin.listTopics()).thenReturn(ltr);
        DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
        when(dtr.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("my-topic", topicDescription)));
        when(mockAdmin.describeTopics(anyCollection())).thenReturn(dtr);

        MockKafkaReconciler kr = prepareReconciler(supplier, List.of(controllersPool(), brokersPool("[10,12]")));
        kr.coTlsPemIdentity = new TlsPemIdentity(null, null);

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        kr.reconcile(status, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Condition warning = warningCondition(status, "SkippedNodeSoleInSyncReplica");
                    assertThat(warning, is(notNullValue()));
                    assertThat(warning.getMessage(), containsString("my-topic-0"));
                    assertThat(warning.getMessage(), not(containsString("my-topic-1")));

                    async.flag();
                })));
    }

    // Internal utility methods
    private static Condition skippedCondition(KafkaStatus status)   {
        return status.getConditions() == null ? null : status.getConditions().stream()
                .filter(condition -> RollingUpdateSkips.ROLLING_UPDATE_SKIPPED_CONDITION_TYPE.equals(condition.getType()))
                .findFirst()
                .orElse(null);
    }

    private static Condition warningCondition(KafkaStatus status, String reason)   {
        return status.getConditions() == null ? null : status.getConditions().stream()
                .filter(condition -> "Warning".equals(condition.getType()) && reason.equals(condition.getReason()))
                .findFirst()
                .orElse(null);
    }

    static class MockKafkaReconciler extends KafkaReconciler   {
        List<String> kafkaNodesConsideredForRolling = new ArrayList<>();

        public MockKafkaReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, List<KafkaNodePool> nodePools, KafkaCluster kafkaCluster) {
            super(reconciliation, kafkaAssembly, nodePools, kafkaCluster, null, null, config, supplier, pfa, vertx);
            this.listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return resolveRollingUpdateSkips(kafkaStatus)
                    .compose(i -> manualRollingUpdate())
                    .compose(i -> rollingUpdate(Map.of()))
                    .compose(i -> podsReady())
                    .compose(i -> skippedNodesSoleInSyncReplicaCheck(kafkaStatus));
        }

        @Override
        protected Future<Void> maybeRollKafka(
                Set<NodeRef> nodes,
                Function<Pod, RestartReasons> podNeedsRestart,
                Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
                Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
                boolean allowReconfiguration
        ) {
            kafkaNodesConsideredForRolling.addAll(nodes.stream().map(NodeRef::podName).toList());
            return Future.succeededFuture();
        }
    }
}
