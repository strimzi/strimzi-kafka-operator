/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
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

import java.util.function.Predicate;

import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_CM_GENERATION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorRollingAfterConfigMapChangeTest {
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

    public Kafka getKafkaCrd() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
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
                .endSpec()
                .build();
    }


    public Pod getPod(StatefulSet sts) {
        return new PodBuilder()
                .withNewMetadataLike(sts.getSpec().getTemplate().getMetadata())
                    .withNewName(KafkaCluster.kafkaClusterName(clusterName) + "-0")
                .endMetadata()
                .withNewSpecLike(sts.getSpec().getTemplate().getSpec())
                .endSpec()
                .build();
    }

    @Test
    public void testWithObsoletePodGeneration(VertxTestContext context) {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS, null);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.updateStatusAsync(any())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null, -1L)));

        ArgumentCaptor<StatefulSet> reconcileStsCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockKafkaSetOps.reconcile(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)), reconcileStsCaptor.capture())).then(invocation -> {
            return Future.succeededFuture(ReconcileResult.patched(invocation.getArgument(2)));
        });

        ArgumentCaptor<StatefulSet> maybeRollingUpdateStsCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        ArgumentCaptor<Predicate<Pod>> isPodToRestartPredicateCaptor = ArgumentCaptor.forClass(Predicate.class);
        when(mockKafkaSetOps.maybeRollingUpdate(maybeRollingUpdateStsCaptor.capture(), isPodToRestartPredicateCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(null)));

        // Mock the SecretOperator
        SecretOperator mockSecretOps = supplier.secretOperations;
        ConfigMap configMap = new ConfigMapBuilder()
                .editOrNewMetadata()
                    .withName("cm")
                    .withNamespace("ns")
                    .withGeneration(500L)
                .endMetadata()
                .build();

        ConfigMap configMapUpdated = new ConfigMapBuilder()
                .editOrNewMetadata()
                    .withName("cm")
                    .withNamespace("ns")
                    .withGeneration(501L)
                .endMetadata()
                .build();

        when(mockCmOps.get(any(), any())).thenReturn(configMap);
        when(mockCmOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.patched(configMapUpdated)));
        when(mockSecretOps.reconcile(any(), any(), any())).then(invocation -> {
            return Future.succeededFuture(ReconcileResult.created(invocation.getArgument(2)));
        });

        KafkaAssemblyOperatorRollingAfterConfigMapChangeTest.MockKafkaAssemblyOperator kao = new KafkaAssemblyOperatorRollingAfterConfigMapChangeTest.MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).setHandler(res -> {
            assertThat(res.succeeded(), is(true));

            Predicate<Pod> isPodToRestart = isPodToRestartPredicateCaptor.getValue();


            assertThat(reconcileStsCaptor.getAllValues().size(), is(1));
            StatefulSet reconcileSts = reconcileStsCaptor.getValue();
            Pod pod = getPod(reconcileSts);
            // lets simulate pod with old sts
            pod.getMetadata().getAnnotations().put(ANNO_STRIMZI_CM_GENERATION, Long.toString(500L));
            assertThat(isPodToRestart.test(pod), is(true));

            async.flag();
        });
    }

    class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.reconcileCas(this::dateSupplier)
                    .compose(state -> state.getKafkaClusterDescription())
                    .compose(state -> state.kafkaAncillaryCm())
                    .compose(state -> state.kafkaStatefulSet())
                    .compose(state -> state.kafkaRollingUpdate())
                    .map((Void) null);
        }
    }
}
