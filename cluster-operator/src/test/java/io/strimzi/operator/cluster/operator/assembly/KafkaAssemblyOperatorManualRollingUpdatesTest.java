/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorManualRollingUpdatesTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_18;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "testns";
    private final String clusterName = "my-cluster";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
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
                    .withName(clusterName)
                    .withNamespace(namespace)
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
            CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null)));

            // This should be removed once StrimziPodSets support Kafka as well and replaced with mocked StrimziPodSet above
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getName()))).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));
        }

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "+UseStrimziPodSets");
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        }

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kao.maybeRollZooKeeperInvocations, is(0));
                    assertThat(kao.maybeRollKafkaInvocations, is(0));

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
                    .withName(clusterName)
                    .withNamespace(namespace)
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
            CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getName()))).thenAnswer(i -> {
                StrimziPodSet zkPodSet = zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null);
                zkPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(zkPodSet);
            });

            // This should be removed once StrimziPodSets support Kafka as well and replaced with mocked StrimziPodSet above
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenAnswer(i -> {
                StatefulSet sts = kafkaCluster.generateStatefulSet(false, null, null);
                sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(sts);
            });
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getName()))).thenAnswer(i -> {
                StatefulSet sts = zkCluster.generateStatefulSet(false, null, null);
                sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(sts);
            });
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenAnswer(i -> {
                StatefulSet sts = kafkaCluster.generateStatefulSet(false, null, null);
                sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
                return Future.succeededFuture(sts);
            });
        }

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "+UseStrimziPodSets");
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        }

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Zookeeper rolling updates
                    assertThat(kao.maybeRollZooKeeperInvocations, is(1));
                    assertThat(kao.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(Collections.singletonList("manual rolling update")));
                    assertThat(kao.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(Collections.singletonList("manual rolling update")));
                    assertThat(kao.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(Collections.singletonList("manual rolling update")));

                    // Verify Kafka rolling updates
                    assertThat(kao.maybeRollKafkaInvocations, is(1));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-0")), is(Collections.singletonList("manual rolling update")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-1")), is(Collections.singletonList("manual rolling update")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-2")), is(Collections.singletonList("manual rolling update")));

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
                    .withName(clusterName)
                    .withNamespace(namespace)
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
            CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null)));

            // This should be removed once StrimziPodSets support Kafka as well and replaced with mocked StrimziPodSet above
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getName()))).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));
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

        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "+UseStrimziPodSets");
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        }

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Zookeeper rolling updates
                    assertThat(kao.maybeRollZooKeeperInvocations, is(1));
                    assertThat(kao.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(nullValue()));
                    assertThat(kao.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(Collections.singletonList("manual rolling update annotation on a pod")));
                    assertThat(kao.zooPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(Collections.singletonList("manual rolling update annotation on a pod")));

                    // Verify Kafka rolling updates
                    assertThat(kao.maybeRollKafkaInvocations, is(1));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-0")), is(Collections.singletonList("manual rolling update annotation on a pod")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-1")), is(Collections.singletonList("manual rolling update annotation on a pod")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-2")), is(Collections.emptyList()));

                    async.flag();
                })));
    }

    @Test
    public void testManualPodCleanupWithSts(VertxTestContext context) {
        manualPodCleanup(context, false);
    }

    @Test
    public void testManualPodCleanupWithPodSets(VertxTestContext context) {
        manualPodCleanup(context, true);
    }

    public void manualPodCleanup(VertxTestContext context, boolean useStrimziPodSets) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
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
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                                    new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                        .endJbodStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewPersistentClaimStorage()
                            .withSize("100Gi")
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        if (useStrimziPodSets) {
            CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> mockPodSetOps = supplier.strimziPodSetOperator;
            when(mockPodSetOps.getAsync(any(), eq(zkCluster.getName()))).thenReturn(Future.succeededFuture(zkCluster.generatePodSet(kafka.getSpec().getZookeeper().getReplicas(), false, null, null, null)));
            when(mockPodSetOps.deleteAsync(any(), any(), eq(zkCluster.getName()), anyBoolean())).thenReturn(Future.succeededFuture());
            when(mockPodSetOps.reconcile(any(), any(), eq(zkCluster.getName()), any())).thenReturn(Future.succeededFuture());

            // This should be removed once StrimziPodSets support Kafka as well and replaced with mocked StrimziPodSet above
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.deleteAsync(any(), any(), eq(kafkaCluster.getName()), anyBoolean())).thenReturn(Future.succeededFuture());
            when(mockStsOps.reconcile(any(), any(), eq(kafkaCluster.getName()), any())).thenReturn(Future.succeededFuture());
        } else {
            StatefulSetOperator mockStsOps = supplier.stsOperations;
            when(mockStsOps.getAsync(any(), eq(zkCluster.getName()))).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.getAsync(any(), eq(kafkaCluster.getName()))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));
            when(mockStsOps.deleteAsync(any(), any(), eq(zkCluster.getName()), anyBoolean())).thenReturn(Future.succeededFuture());
            when(mockStsOps.deleteAsync(any(), any(), eq(kafkaCluster.getName()), anyBoolean())).thenReturn(Future.succeededFuture());
            when(mockStsOps.reconcile(any(), any(), eq(zkCluster.getName()), any())).thenReturn(Future.succeededFuture());
            when(mockStsOps.reconcile(any(), any(), eq(kafkaCluster.getName()), any())).thenReturn(Future.succeededFuture());
        }

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithName("my-cluster-zookeeper-0"));
            pods.add(podWithNameAndAnnotations("my-cluster-zookeeper-1", Collections.singletonMap(AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")));
            pods.add(podWithName("my-cluster-zookeeper-2"));

            return Future.succeededFuture(pods);
        });
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            pods.add(podWithNameAndAnnotations("my-cluster-kafka-0", Collections.singletonMap(AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")));
            pods.add(podWithName("my-cluster-kafka-1"));
            pods.add(podWithName("my-cluster-kafka-2"));

            return Future.succeededFuture(pods);
        });
        when(mockPodOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(zkCluster.generatePersistentVolumeClaims()));
        when(mockPvcOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage())));

        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "+UseStrimziPodSets");
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        }

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify Zookeeper rolling updates
                    assertThat(kao.cleanedPodsCount, is(2));
                    assertThat(kao.cleanedPods, Matchers.containsInAnyOrder("my-cluster-zookeeper-1", "my-cluster-kafka-0"));
                    assertThat(kao.cleanedPvcs, Matchers.containsInAnyOrder("data-my-cluster-zookeeper-1", "data-0-my-cluster-kafka-0", "data-1-my-cluster-kafka-0"));
                    assertThat(kao.createdPvcs, Matchers.containsInAnyOrder("data-my-cluster-zookeeper-1", "data-0-my-cluster-kafka-0", "data-1-my-cluster-kafka-0"));

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

    class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        int maybeRollKafkaInvocations = 0;
        Function<Pod, List<String>> kafkaPodNeedsRestart = null;

        int maybeRollZooKeeperInvocations = 0;
        Function<Pod, List<String>> zooPodNeedsRestart = null;

        int cleanedPodsCount = 0;
        List<String> cleanedPods = new ArrayList<>();
        List<String> cleanedPvcs = new ArrayList<>();
        List<String> createdPvcs = new ArrayList<>();

        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            return new MockReconciliationState(reconciliation, kafkaAssembly);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return Future.succeededFuture(reconcileState)
                    .compose(state -> state.getKafkaClusterDescription())
                    .compose(state -> state.getZookeeperDescription())
                    .compose(state -> state.zkManualPodCleaning())
                    .compose(state -> state.zkManualRollingUpdate())
                    .compose(state -> state.kafkaManualPodCleaning())
                    .compose(state -> state.kafkaManualRollingUpdate())
                    .mapEmpty();
        }

        class MockReconciliationState extends KafkaAssemblyOperator.ReconciliationState {
            MockReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
                super(reconciliation, kafkaAssembly);
            }

            Future<Void> maybeRollZooKeeper(Function<Pod, List<String>> podNeedsRestart) {
                maybeRollZooKeeperInvocations++;
                zooPodNeedsRestart = podNeedsRestart;
                return Future.succeededFuture();
            }

            Future<Void> maybeRollKafka(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
                maybeRollKafkaInvocations++;
                kafkaPodNeedsRestart = podNeedsRestart;
                return Future.succeededFuture();
            }

            Future<Void> cleanPodAndPvc(String podName, List<PersistentVolumeClaim> createPvcs, List<PersistentVolumeClaim> deletePvcs) {
                cleanedPodsCount++;
                cleanedPods.add(podName);
                cleanedPvcs.addAll(deletePvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList()));
                createdPvcs.addAll(createPvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList()));

                return Future.succeededFuture();
            }
        }
    }
}
