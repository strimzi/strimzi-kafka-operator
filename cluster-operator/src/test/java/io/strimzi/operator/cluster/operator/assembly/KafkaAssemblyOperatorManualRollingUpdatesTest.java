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
import io.strimzi.api.kafka.model.kafka.Storage;
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
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;
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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorManualRollingUpdatesTest {
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static KafkaVersionChange VERSION_CHANGE = new KafkaVersionChange(
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion().protocolVersion(),
            VERSIONS.defaultVersion().messageVersion(),
            VERSIONS.defaultVersion().metadataVersion()
    );
    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "my-cluster";

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
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
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
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, SHARED_ENV_PROVIDER);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, podNum -> null)));
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSets(false, null, null, brokerId -> null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockZooKeeperReconciler zr = new MockZooKeeperReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                VERSION_CHANGE,
                null,
                0,
                null);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                null,
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                zr,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(zr.maybeRollZooKeeperInvocations, is(0));
                    assertThat(kr.maybeRollKafkaInvocations, is(0));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(0));

                    async.flag();
                })));
    }

    @Test
    public void testManualRollingUpdateWithPodSets(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
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
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, SHARED_ENV_PROVIDER);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenAnswer(i -> {
            StrimziPodSet zkPodSet = zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, podNum -> null);
            zkPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return Future.succeededFuture(zkPodSet);
        });
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            StrimziPodSet kafkaPodSet = kafkaCluster.generatePodSets(false, null, null, brokerId -> null).stream().filter(ps -> kafkaCluster.getComponentName().equals(ps.getMetadata().getName())).findFirst().orElseThrow();
            kafkaPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return Future.succeededFuture(List.of(kafkaPodSet));
        });

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockZooKeeperReconciler zr = new MockZooKeeperReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                VERSION_CHANGE,
                null,
                0,
                null);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                null,
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                zr,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Zookeeper rolling updates
                    assertThat(zr.maybeRollZooKeeperInvocations, is(1));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(Collections.singletonList("manual rolling update")));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(Collections.singletonList("manual rolling update")));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(Collections.singletonList("manual rolling update")));

                    // Verify Kafka rolling updates
                    assertThat(kr.maybeRollKafkaInvocations, is(1));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(3));
                    assertThat(kr.kafkaNodesNeedRestart, is(List.of("my-cluster-kafka-0", "my-cluster-kafka-1", "my-cluster-kafka-2")));
                    assertThat(kr.kafkaRestartReasons.apply(podWithName("anyName")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
    }

    @Test
    public void testManualPodRollingUpdateWithPodSets(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
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
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, SHARED_ENV_PROVIDER);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, podNum -> null)));
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSets(false, null, null, brokerId -> null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithName("my-cluster-zookeeper-0"));
            pods.add(podWithNameAndAnnotations("my-cluster-zookeeper-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-zookeeper-2", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));

            return Future.succeededFuture(pods);
        });
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithNameAndAnnotations("my-cluster-kafka-0", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-kafka-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithName("my-cluster-kafka-2"));

            return Future.succeededFuture(pods);
        });

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockZooKeeperReconciler zr = new MockZooKeeperReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                VERSION_CHANGE,
                null,
                0,
                null);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                null,
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                zr,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Zookeeper rolling updates
                    assertThat(zr.maybeRollZooKeeperInvocations, is(1));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(nullValue()));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(List.of("manual rolling update annotation on a pod")));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(List.of("manual rolling update annotation on a pod")));

                    // Verify Kafka rolling updates
                    assertThat(kr.maybeRollKafkaInvocations, is(1));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(2));
                    assertThat(kr.kafkaNodesNeedRestart, is(List.of("my-cluster-kafka-0", "my-cluster-kafka-1")));
                    assertThat(kr.kafkaRestartReasons.apply(podWithName("anyName")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
    }

    @Test
    public void testManualPodRollingUpdateWithNodePools(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
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
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        KafkaNodePool poolA = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-a")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();
        KafkaNodePool poolB = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-b")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(poolA, poolB), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, SHARED_ENV_PROVIDER);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, podNum -> null)));
        when(mockPodSetOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            List<StrimziPodSet> podSets = kafkaCluster.generatePodSets(false, null, null, brokerId -> null);
            podSets.get(1).getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return Future.succeededFuture(podSets);
        });

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithName("my-cluster-zookeeper-0"));
            pods.add(podWithNameAndAnnotations("my-cluster-zookeeper-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithNameAndAnnotations("my-cluster-zookeeper-2", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));

            return Future.succeededFuture(pods);
        });
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithName("my-cluster-pool-a-0"));
            pods.add(podWithNameAndAnnotations("my-cluster-pool-a-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")));
            pods.add(podWithName("my-cluster-pool-a-2"));
            pods.add(podWithName("my-cluster-pool-b-3"));
            pods.add(podWithName("my-cluster-pool-b-4"));

            return Future.succeededFuture(pods);
        });

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        MockZooKeeperReconciler zr = new MockZooKeeperReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                VERSION_CHANGE,
                null,
                0,
                null);

        MockKafkaReconciler kr = new MockKafkaReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                vertx,
                config,
                supplier,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                kafka,
                List.of(poolA, poolB),
                kafkaCluster,
                null,
                null);

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config,
                zr,
                kr);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Zookeeper rolling updates
                    assertThat(zr.maybeRollZooKeeperInvocations, is(1));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(nullValue()));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(List.of("manual rolling update annotation on a pod")));
                    assertThat(zr.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(List.of("manual rolling update annotation on a pod")));

                    // Verify Kafka rolling updates
                    assertThat(kr.maybeRollKafkaInvocations, is(1));
                    assertThat(kr.kafkaNodesNeedRestart.size(), is(3));
                    assertThat(kr.kafkaNodesNeedRestart, hasItems("my-cluster-pool-a-1", "my-cluster-pool-b-3", "my-cluster-pool-b-4"));
                    assertThat(kr.kafkaRestartReasons.apply(podWithName("anyName")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
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
        ZooKeeperReconciler mockZooKeeperReconciler;
        KafkaReconciler mockKafkaReconciler;

        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config, ZooKeeperReconciler mockZooKeeperReconciler, KafkaReconciler mockKafkaReconciler) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
            this.mockZooKeeperReconciler = mockZooKeeperReconciler;
            this.mockKafkaReconciler = mockKafkaReconciler;
        }

        ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            return new MockReconciliationState(reconciliation, kafkaAssembly);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return Future.succeededFuture(reconcileState)
                    .compose(state -> state.reconcileZooKeeper(this.clock))
                    .compose(state -> state.reconcileKafka(this.clock))
                    .mapEmpty();
        }

        class MockReconciliationState extends KafkaAssemblyOperator.ReconciliationState {
            MockReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
                super(reconciliation, kafkaAssembly);
            }

            @Override
            Future<ZooKeeperReconciler> zooKeeperReconciler()    {
                return Future.succeededFuture(mockZooKeeperReconciler);
            }

            @Override
            Future<KafkaReconciler> kafkaReconciler()    {
                return Future.succeededFuture(mockKafkaReconciler);
            }
        }
    }

    static class MockZooKeeperReconciler extends ZooKeeperReconciler   {
        int maybeRollZooKeeperInvocations = 0;
        Function<Pod, List<String>> zooPodNeedsRestart = null;

        public MockZooKeeperReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, KafkaVersionChange versionChange, Storage oldStorage, int currentReplicas, ClusterCa clusterCa) {
            super(reconciliation, vertx, config, supplier, pfa, kafkaAssembly, versionChange, oldStorage, currentReplicas, clusterCa, false);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return manualRollingUpdate();
        }

        @Override
        Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart, TlsPemIdentity coTlsPemIdentity) {
            maybeRollZooKeeperInvocations++;
            zooPodNeedsRestart = podNeedsRestart;
            return Future.succeededFuture();
        }
    }

    static class MockKafkaReconciler extends KafkaReconciler   {
        int maybeRollKafkaInvocations = 0;
        Function<Pod, RestartReasons> kafkaRestartReasons = null;
        List<String> kafkaNodesNeedRestart = new ArrayList<>();

        public MockKafkaReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, List<KafkaNodePool> nodePools, KafkaCluster kafkaCluster, ClusterCa clusterCa, ClientsCa clientsCa) {
            super(reconciliation, kafkaAssembly, nodePools, kafkaCluster, clusterCa, clientsCa, config, supplier, pfa, vertx, new KafkaMetadataStateManager(reconciliation, kafkaAssembly, config.featureGates().useKRaftEnabled()));
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
            return Future.succeededFuture();
        }
    }
}
