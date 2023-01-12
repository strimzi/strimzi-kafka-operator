/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
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
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
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
    private final static KafkaVersionChange VERSION_CHANGE = new KafkaVersionChange(
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion().protocolVersion(),
            VERSIONS.defaultVersion().messageVersion()
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
    public void testNoManualRollingUpdateWithSts(VertxTestContext context)  {
        noManualRollingUpdate(context, false);
    }

    @Test
    public void testNoManualRollingUpdateWithPodSets(VertxTestContext context)  {
        noManualRollingUpdate(context, true);
    }

    public void noManualRollingUpdate(VertxTestContext context, boolean useStrimziPodSets) {
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        if (useStrimziPodSets) {
            StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null)));
            when(mockPodSetOps.getAsync(any(), eq(kafkaCluster.getComponentName()))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSet(kafka.getSpec().getKafka().getReplicas(), false, null, null, brokerId -> null)));

            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getComponentName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null, null)));

            StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        }

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "-UseStrimziPodSets");
        }

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
                VERSION_CHANGE,
                null,
                0,
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

                    async.flag();
                })));
    }

    @Test
    public void testManualRollingUpdateWithSts(VertxTestContext context) {
        manualRollingUpdate(context, false);
    }

    @Test
    public void testManualRollingUpdateWithPodSets(VertxTestContext context) {
        manualRollingUpdate(context, true);
    }

    public void manualRollingUpdate(VertxTestContext context, boolean useStrimziPodSets) {
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        if (useStrimziPodSets) {
            StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenAnswer(i -> {
                StrimziPodSet zkPodSet = zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null);
                zkPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(zkPodSet);
            });
            when(mockPodSetOps.getAsync(any(), eq(kafkaCluster.getComponentName()))).thenAnswer(i -> {
                StrimziPodSet kafkaPodSet = kafkaCluster.generatePodSet(kafka.getSpec().getKafka().getReplicas(), false, null, null, brokerId -> null);
                kafkaPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(kafkaPodSet);
            });

            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenAnswer(i -> {
                StatefulSet sts = zkCluster.generateStatefulSet(false, null, null);
                sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(sts);
            });
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getComponentName()))).thenAnswer(i -> {
                StatefulSet sts = kafkaCluster.generateStatefulSet(false, null, null, null);
                sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(sts);
            });

            StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        }

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "-UseStrimziPodSets");
        }

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
                VERSION_CHANGE,
                null,
                0,
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
                    assertThat(kr.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-0")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));
                    assertThat(kr.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-1")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));
                    assertThat(kr.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-2")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));

                    async.flag();
                })));
    }

    @Test
    public void testManualPodRollingUpdateWithSts(VertxTestContext context) {
        manualPodRollingUpdate(context, false);
    }

    @Test
    public void testManualPodRollingUpdateWithPodSets(VertxTestContext context) {
        manualPodRollingUpdate(context, true);
    }

    public void manualPodRollingUpdate(VertxTestContext context, boolean useStrimziPodSets) {
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        if (useStrimziPodSets) {
            StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null)));
            when(mockPodSetOps.getAsync(any(), eq(kafkaCluster.getComponentName()))).thenReturn(Future.succeededFuture(kafkaCluster.generatePodSet(kafka.getSpec().getKafka().getReplicas(), false, null, null, brokerId -> null)));

            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getComponentName()))).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getComponentName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null, null)));

            StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        }

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

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "-UseStrimziPodSets");
        }

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
                VERSION_CHANGE,
                null,
                0,
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
                    assertThat(kr.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-0")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));
                    assertThat(kr.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-1")), is(RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE)));
                    assertThat(kr.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-2")), is(RestartReasons.empty()));

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
            super(reconciliation, vertx, config, supplier, pfa, kafkaAssembly, versionChange, oldStorage, currentReplicas, clusterCa);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return manualRollingUpdate();
        }

        @Override
        Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart) {
            maybeRollZooKeeperInvocations++;
            zooPodNeedsRestart = podNeedsRestart;
            return Future.succeededFuture();
        }
    }

    static class MockKafkaReconciler extends KafkaReconciler   {
        int maybeRollKafkaInvocations = 0;
        Function<Pod, RestartReasons> kafkaPodNeedsRestart = null;

        public MockKafkaReconciler(Reconciliation reconciliation, Vertx vertx, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, PlatformFeaturesAvailability pfa, Kafka kafkaAssembly, KafkaVersionChange versionChange, Storage oldStorage, int currentReplicas, ClusterCa clusterCa, ClientsCa clientsCa) {
            super(reconciliation, kafkaAssembly, oldStorage, currentReplicas, clusterCa, clientsCa, versionChange, config, supplier, pfa, vertx);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return manualRollingUpdate();
        }

        @Override
        protected Future<Void> maybeRollKafka(
                int replicas,
                Function<Pod, RestartReasons> podNeedsRestart,
                Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
                Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
                boolean allowReconfiguration
        ) {
            maybeRollKafkaInvocations++;
            kafkaPodNeedsRestart = podNeedsRestart;
            return Future.succeededFuture();
        }
    }
}
