/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CaReconcilerZooBasedTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-kafka";
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                    .endEntityOperator()
                    .withNewCruiseControl()
                    .endCruiseControl()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

    private static final OpenSslCertManager CERT_MANAGER = new OpenSslCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    private WorkerExecutor sharedWorkerExecutor;

    @BeforeEach
    public void setup(Vertx vertx) {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterEach
    public void teardown() {
        sharedWorkerExecutor.close();
    }

    @Test
    public void testClusterCAKeyNotTrusted(Vertx vertx, VertxTestContext context) {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> {
            Secret s = clusterCaCert.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> {
            Secret s = clusterCaKey.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0", Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0");

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            // adding a terminating Cruise Control pod to test that it's skipped during the key generation check
            Pod ccPod = podWithNameAndAnnotations("my-kafka-cruise-control", generationAnnotations);
            ccPod.getMetadata().setDeletionTimestamp("2023-06-08T16:23:18Z");
            pods.add(ccPod);
            // adding ZooKeeper and Kafka pods with old CA cert and key generation
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-2", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-2", generationAnnotations));
            return Future.succeededFuture(pods);
        });

        Checkpoint async = context.checkpoint();

        CaReconciler caReconciler = new CaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        caReconciler
                .reconcileCas(Clock.systemUTC())
                .compose(i -> caReconciler.verifyClusterCaFullyTrustedAndUsed())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(caReconciler.isClusterCaNeedFullTrust, is(true));
                    async.flag();
                })));
    }

    @Test
    public void testRollingReasonsWithClusterCAKeyNotTrusted(Vertx vertx, VertxTestContext context) {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> {
            Secret s = clusterCaCert.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> {
            Secret s = clusterCaKey.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0", Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0");

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            // adding a terminating Cruise Control pod to test that it's skipped during the key generation check
            Pod ccPod = podWithNameAndAnnotations("my-kafka-cruise-control", generationAnnotations);
            ccPod.getMetadata().setDeletionTimestamp("2023-06-08T16:23:18Z");
            pods.add(ccPod);
            // adding ZooKeeper and Kafka pods with old CA cert and key generation
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-2", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-2", generationAnnotations));
            return Future.succeededFuture(pods);
        });

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-kafka-entity-operator", deploymentWithName("my-kafka-entity-operator"));
        deps.put("my-kafka-cruise-control", deploymentWithName("my-kafka-cruise-control"));
        deps.put("my-kafka-kafka-exporter", deploymentWithName("my-kafka-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1))));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(mockCaReconciler.isClusterCaNeedFullTrust, is(true));
                    assertThat(mockCaReconciler.zkPodRestartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    assertThat(mockCaReconciler.kPodRollReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    assertThat(mockCaReconciler.deploymentRollReason.size() == 3, is(true));
                    for (String reason: mockCaReconciler.deploymentRollReason) {
                        assertThat(reason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true));
                    }
                    async.flag();
                })));
    }

    static class MockCaReconciler extends CaReconciler {

        RestartReasons zkPodRestartReasons;
        RestartReasons kPodRollReasons;
        List<String> deploymentRollReason = new ArrayList<>();

        public MockCaReconciler(Reconciliation reconciliation, Kafka kafkaCr, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, Vertx vertx, CertManager certManager, PasswordGenerator passwordGenerator) {
            super(reconciliation, kafkaCr, config, supplier, vertx, certManager, passwordGenerator);
        }

        @Override
        Future<Void> verifyClusterCaFullyTrustedAndUsed() {
            // assuming the CA key is not trusted
            this.isClusterCaNeedFullTrust = true;
            this.isClusterCaFullyUsed = false;
            return Future.succeededFuture();
        }

        @Override
        Future<Integer> getZooKeeperReplicas() {
            return Future.succeededFuture(3);
        }

        @Override
        Future<Void> maybeRollZookeeper(int replicas, RestartReasons podRestartReasons, TlsPemIdentity coTlsPemIdentity) {
            this.zkPodRestartReasons = podRestartReasons;
            return Future.succeededFuture();
        }

        @Override
        Future<Set<NodeRef>> getKafkaReplicas() {
            Set<NodeRef> nodes = new HashSet<>();
            nodes.add(ReconcilerUtils.nodeFromPod(podWithName("my-kafka-kafka-0")));
            nodes.add(ReconcilerUtils.nodeFromPod(podWithName("my-kafka-kafka-1")));
            nodes.add(ReconcilerUtils.nodeFromPod(podWithName("my-kafka-kafka-2")));
            return Future.succeededFuture(nodes);
        }

        @Override
        Future<Void> rollKafkaBrokers(Set<NodeRef> nodes, RestartReasons podRollReasons, TlsPemIdentity coTlsPemIdentity) {
            this.kPodRollReasons = podRollReasons;
            return Future.succeededFuture();
        }

        @Override
        Future<Void> rollDeploymentIfExists(String deploymentName, String reason) {
            return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            this.deploymentRollReason.add(reason);
                        }
                        return Future.succeededFuture();
                    });
        }

        @Override
        Future<Void> maybeRemoveOldClusterCaCertificates() {
            return Future.succeededFuture();
        }
    }

    public static Pod podWithName(String name) {
        return podWithNameAndAnnotations(name, Collections.emptyMap());
    }

    public static Pod podWithNameAndAnnotations(String name, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME))
                .endMetadata()
                .build();
    }

    public static Deployment deploymentWithName(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }
}
