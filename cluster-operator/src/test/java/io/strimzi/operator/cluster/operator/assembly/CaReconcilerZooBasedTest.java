/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Reconciliation;
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
import java.util.List;
import java.util.Map;

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

    public static Pod podWithNameAndAnnotations(String name, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME))
                .endMetadata()
                .build();
    }
}
