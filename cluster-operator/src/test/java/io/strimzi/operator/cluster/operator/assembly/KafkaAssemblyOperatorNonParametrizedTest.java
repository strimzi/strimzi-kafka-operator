/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorNonParametrizedTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private final OpenSslCertManager certManager = new OpenSslCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

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
    public void testCustomLabelsAndAnnotations(VertxTestContext context) {
        Map<String, String> labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        Map<String, String> annos = new HashMap<>(2);
        annos.put("anno1", "value3");
        annos.put("anno2", "value4");

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withNewTemplate()
                            .withNewClusterCaCert()
                                .withNewMetadata()
                                    .withAnnotations(annos)
                                    .withLabels(labels)
                                .endMetadata()
                            .endClusterCaCert()
                        .endTemplate()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        PodOperator podOps = supplier.podOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(ClusterOperator.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.new ReconciliationState(reconciliation, kafka).reconcileCas(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(clusterCaCert.getAllValues(), hasSize(1));
                    assertThat(clusterCaKey.getAllValues(), hasSize(1));
                    assertThat(clientsCaCert.getAllValues(), hasSize(1));
                    assertThat(clientsCaKey.getAllValues(), hasSize(1));

                    Secret clusterCaCertSecret = clusterCaCert.getValue();
                    Secret clusterCaKeySecret = clusterCaKey.getValue();
                    Secret clientsCaCertSecret = clientsCaCert.getValue();
                    Secret clientsCaKeySecret = clientsCaKey.getValue();

                    for (Map.Entry<String, String> entry : annos.entrySet()) {
                        assertThat(clusterCaCertSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
                        assertThat(clusterCaKeySecret.getMetadata().getAnnotations(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaCertSecret.getMetadata().getAnnotations(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaKeySecret.getMetadata().getAnnotations(), not(hasEntry(entry.getKey(), entry.getValue())));
                    }

                    for (Map.Entry<String, String> entry : labels.entrySet()) {
                        assertThat(clusterCaCertSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
                        assertThat(clusterCaKeySecret.getMetadata().getLabels(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaCertSecret.getMetadata().getLabels(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaKeySecret.getMetadata().getLabels(), not(hasEntry(entry.getKey(), entry.getValue())));
                    }

                    async.flag();
                })));
    }

    @Test
    public void testClusterCASecretsWithoutOwnerReference(VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder()
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
                    .withClusterCa(caConfig)
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withKind(kafka.getKind())
                .withApiVersion(kafka.getApiVersion())
                .withName(kafka.getMetadata().getName())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        PodOperator podOps = supplier.podOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(ClusterOperator.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.new ReconciliationState(reconciliation, kafka).reconcileCas(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(clusterCaCert.getAllValues(), hasSize(1));
                    assertThat(clusterCaKey.getAllValues(), hasSize(1));
                    assertThat(clientsCaCert.getAllValues(), hasSize(1));
                    assertThat(clientsCaKey.getAllValues(), hasSize(1));

                    Secret clusterCaCertSecret = clusterCaCert.getValue();
                    Secret clusterCaKeySecret = clusterCaKey.getValue();
                    Secret clientsCaCertSecret = clientsCaCert.getValue();
                    Secret clientsCaKeySecret = clientsCaKey.getValue();

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences(), hasSize(1));

                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));

                    async.flag();
                })));
    }

    @Test
    public void testClientsCASecretsWithoutOwnerReference(VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder()
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
                    .withClientsCa(caConfig)
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        
        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withKind(kafka.getKind())
                .withApiVersion(kafka.getApiVersion())
                .withName(kafka.getMetadata().getName())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
        
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        PodOperator podOps = supplier.podOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(ClusterOperator.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.new ReconciliationState(reconciliation, kafka).reconcileCas(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(clusterCaCert.getAllValues(), hasSize(1));
                    assertThat(clusterCaKey.getAllValues(), hasSize(1));
                    assertThat(clientsCaCert.getAllValues(), hasSize(1));
                    assertThat(clientsCaKey.getAllValues(), hasSize(1));

                    Secret clusterCaCertSecret = clusterCaCert.getValue();
                    Secret clusterCaKeySecret = clusterCaKey.getValue();
                    Secret clientsCaCertSecret = clientsCaCert.getValue();
                    Secret clientsCaKeySecret = clientsCaKey.getValue();

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences(), hasSize(0));

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));

                    async.flag();
                })));
    }

    @Test
    public void testDeleteClusterRoleBindings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(any(), eq(KafkaResources.initContainerClusterRoleBindingName(NAME, NAMESPACE)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.delete(reconciliation)
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(desiredCrb.getValue(), is(nullValue()));
                    Mockito.verify(mockCrbOps, times(1)).reconcile(any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testSelectorLabels(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
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
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = new ClusterOperatorConfig(
                singleton("dummy"),
                60_000,
                120_000,
                300_000,
                false,
                true,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                null,
                null,
                null,
                null,
                Labels.fromMap(Map.of("selectorLabel", "value")),
                "",
                10,
                10_000,
                30,
                false,
                1024,
                "cluster-operator-name",
                ClusterOperatorConfig.DEFAULT_POD_SECURITY_PROVIDER_CLASS, null);

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION), certManager, passwordGenerator,
                supplier, config);
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // The resource labels don't match the selector labels => the reconciliation should exit right on
                    // beginning with success. It should not reconcile any resources other than getting the Kafka
                    // resource it self.
                    verifyNoInteractions(
                            supplier.stsOperations,
                            supplier.serviceOperations,
                            supplier.secretOperations,
                            supplier.configMapOperations,
                            supplier.podOperations,
                            supplier.podDisruptionBudgetOperator,
                            supplier.deploymentOperations
                    );

                    async.flag();
                })));
    }
}
