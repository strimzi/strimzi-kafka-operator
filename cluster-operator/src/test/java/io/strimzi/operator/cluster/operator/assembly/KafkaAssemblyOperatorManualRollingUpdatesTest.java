/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
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
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.text.ParseException;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorManualRollingUpdatesTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
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
    public void testNoManualRollingUpdate(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                            .endGenericKafkaListener()
                        .endListeners()
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StatefulSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        StatefulSetOperator mockZkSetOps = supplier.zkSetOperations;
        when(mockZkSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any())).thenReturn(Future.succeededFuture());
        when(mockKafkaOps.updateStatusAsync(any())).thenReturn(Future.succeededFuture());

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Mockito.verify(mockKafkaSetOps, never()).maybeRollingUpdate(any(), any());
                    Mockito.verify(mockZkSetOps, never()).maybeRollingUpdate(any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testStatefulSetManualRollingUpdate(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                            .endGenericKafkaListener()
                        .endListeners()
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StatefulSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(any(), any())).thenAnswer(i -> {
            StatefulSet sts = kafkaCluster.generateStatefulSet(false, null, null);
            sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return Future.succeededFuture(sts);
        });

        StatefulSetOperator mockZkSetOps = supplier.zkSetOperations;
        when(mockZkSetOps.getAsync(any(), any())).thenAnswer(i -> {
            StatefulSet sts = zkCluster.generateStatefulSet(false, null, null);
            sts.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
            return Future.succeededFuture(sts);
        });
        ArgumentCaptor<Function<Pod, List<String>>> zkNeedsRestartCaptor = ArgumentCaptor.forClass(Function.class);
        when(mockZkSetOps.maybeRollingUpdate(any(), zkNeedsRestartCaptor.capture())).thenReturn(Future.succeededFuture());

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(zkCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(mockPodOps.listAsync(any(), eq(kafkaCluster.getSelectorLabels()))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any())).thenReturn(Future.succeededFuture());

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
                    Mockito.verify(mockZkSetOps, times(1)).maybeRollingUpdate(any(), any());
                    Function<Pod, List<String>> zkPodNeedsRestart = zkNeedsRestartCaptor.getValue();
                    assertThat(zkPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(Collections.singletonList("manual rolling update")));
                    assertThat(zkPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(Collections.singletonList("manual rolling update")));
                    assertThat(zkPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(Collections.singletonList("manual rolling update")));

                    // Verify Kafka rolling updates
                    assertThat(kao.maybeRollKafkaInvocations, is(1));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-0")), is(Collections.singletonList("manual rolling update")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-1")), is(Collections.singletonList("manual rolling update")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-2")), is(Collections.singletonList("manual rolling update")));

                    async.flag();
                })));
    }

    @Test
    public void testPodManualRollingUpdate(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                            .endGenericKafkaListener()
                        .endListeners()
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        StatefulSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        StatefulSetOperator mockZkSetOps = supplier.zkSetOperations;
        when(mockZkSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(zkCluster.generateStatefulSet(false, null, null)));
        ArgumentCaptor<Function<Pod, List<String>>> zkNeedsRestartCaptor = ArgumentCaptor.forClass(Function.class);
        when(mockZkSetOps.maybeRollingUpdate(any(), zkNeedsRestartCaptor.capture())).thenReturn(Future.succeededFuture());

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
        when(mockKafkaOps.updateStatusAsync(any())).thenReturn(Future.succeededFuture());

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
                    Mockito.verify(mockZkSetOps, times(1)).maybeRollingUpdate(any(), any());
                    Function<Pod, List<String>> zkPodNeedsRestart = zkNeedsRestartCaptor.getValue();
                    assertThat(zkPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-0")), is(nullValue()));
                    assertThat(zkPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-1")), is(Collections.singletonList("manual rolling update annotation on a pod")));
                    assertThat(zkPodNeedsRestart.apply(podWithName("my-cluster-zookeeper-2")), is(Collections.singletonList("manual rolling update annotation on a pod")));

                    // Verify Kafka rolling updates
                    assertThat(kao.maybeRollKafkaInvocations, is(1));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-0")), is(Collections.singletonList("manual rolling update annotation on a pod")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-1")), is(Collections.singletonList("manual rolling update annotation on a pod")));
                    assertThat(kao.kafkaPodNeedsRestart.apply(podWithName("my-cluster-kafka-2")), is(nullValue()));

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
                    .compose(state -> state.zkManualRollingUpdate())
                    .compose(state -> state.kafkaManualRollingUpdate())
                    .mapEmpty();
        }

        class MockReconciliationState extends KafkaAssemblyOperator.ReconciliationState {
            MockReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
                super(reconciliation, kafkaAssembly);
            }

            Future<Void> maybeRollKafka(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
                maybeRollKafkaInvocations++;
                kafkaPodNeedsRestart = podNeedsRestart;
                return Future.succeededFuture();
            }
        }
    }
}
