/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
@ParallelSuite
public class KafkaAssemblyOperatorNonParametrizedTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private static Vertx vertx;
    private OpenSslCertManager certManager = new OpenSslCertManager();
    private PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @ParallelTest
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

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaCertSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.new ReconciliationState(reconciliation, kafka).reconcileCas(() -> new Date())
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

    @ParallelTest
    public void testClusterCASecretsWithoutOwnerReference(VertxTestContext context) {
        OwnerReference ownerReference = new OwnerReferenceBuilder()
                        .withKind("Kafka")
                        .withName(NAME)
                        .withBlockOwnerDeletion(false)
                        .withController(false)
                        .build();

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

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaCertSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.new ReconciliationState(reconciliation, kafka).reconcileCas(() -> new Date())
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

    @ParallelTest
    public void testClientsCASecretsWithoutOwnerReference(VertxTestContext context) {
        OwnerReference ownerReference = new OwnerReferenceBuilder()
                        .withKind("Kafka")
                        .withName(NAME)
                        .withBlockOwnerDeletion(false)
                        .withController(false)
                        .build();

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

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaCertSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.new ReconciliationState(reconciliation, kafka).reconcileCas(() -> new Date())
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

    @ParallelTest
    public void testDeleteClusterRoleBindings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(eq(KafkaResources.initContainerClusterRoleBindingName(NAME, NAMESPACE)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.delete(reconciliation)
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(desiredCrb.getValue(), is(nullValue()));
                    Mockito.verify(mockCrbOps, times(1)).reconcile(any(), any());

                    async.flag();
                })));
    }

    @ParallelTest
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
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config = new ClusterOperatorConfig(
                singleton("dummy"),
                60_000,
                120_000,
                300_000,
                false,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                null,
                null,
                null,
                null,
                ClusterOperatorConfig.RbacScope.CLUSTER,
                Labels.fromMap(Map.of("selectorLabel", "value")),
                "");

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_19), certManager, passwordGenerator,
                supplier, config);
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // The resource labels don't match the selector labels => the reconciliation should exit right on
                    // beginning with success. It should not reconcile any resources other than getting the Kafka
                    // resource it self.
                    verifyZeroInteractions(
                            supplier.kafkaSetOperations,
                            supplier.zkSetOperations,
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

    @ParallelTest
    public void testIngressV1Beta1(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("ingress")
                                .withPort(9094)
                                .withTls(true)
                                .withType(KafkaListenerType.INGRESS)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("bootstrap.mydomain.tld")
                                    .endBootstrap()
                                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(0).withHost("broker-0.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1).withHost("broker-1.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(2).withHost("broker-2.mydomain.tld").build())
                                .endConfiguration()
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
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the KafkaSet operations
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(NAMESPACE), eq(KafkaCluster.kafkaClusterName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock the Pod operations
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock ingress v1beta1 ops
        IngressV1Beta1Operator mockIngressV1Beta1ops = supplier.ingressV1Beta1Operations;
        ArgumentCaptor<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingressV1Beta1Captor = ArgumentCaptor.forClass(io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress.class);
        when(mockIngressV1Beta1ops.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressV1Beta1ops.reconcile(anyString(), anyString(), ingressV1Beta1Captor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress())));
        when(mockIngressV1Beta1ops.hasIngressAddress(eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock ingress v1 ops
        IngressOperator mockIngressOps = supplier.ingressOperations;
        ArgumentCaptor<Ingress> ingressCaptor = ArgumentCaptor.forClass(Ingress.class);
        when(mockIngressOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressOps.reconcile(anyString(), anyString(), ingressCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Ingress())));
        when(mockIngressOps.hasIngressAddress(eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForIngressTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(ingressCaptor.getAllValues().size(), is(0));
                    assertThat(ingressV1Beta1Captor.getAllValues().size(), is(4));

                    verify(mockIngressOps, never()).list(any(), any());
                    verify(mockIngressOps, never()).reconcile(any(), any(), any());
                    verify(mockIngressOps, never()).hasIngressAddress(any(), any(), anyLong(), anyLong());

                    async.flag();
                })));
    }

    @ParallelTest
    public void testIngressV1(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("ingress")
                                .withPort(9094)
                                .withTls(true)
                                .withType(KafkaListenerType.INGRESS)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("bootstrap.mydomain.tld")
                                    .endBootstrap()
                                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(0).withHost("broker-0.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1).withHost("broker-1.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(2).withHost("broker-2.mydomain.tld").build())
                                .endConfiguration()
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
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the KafkaSet operations
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(NAMESPACE), eq(KafkaCluster.kafkaClusterName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock the Pod operations
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock ingress v1beta1 ops
        IngressV1Beta1Operator mockIngressV1Beta1ops = supplier.ingressV1Beta1Operations;
        ArgumentCaptor<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingressV1Beta1Captor = ArgumentCaptor.forClass(io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress.class);
        when(mockIngressV1Beta1ops.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressV1Beta1ops.reconcile(anyString(), anyString(), ingressV1Beta1Captor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress())));
        when(mockIngressV1Beta1ops.hasIngressAddress(eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock ingress v1 ops
        IngressOperator mockIngressOps = supplier.ingressOperations;
        ArgumentCaptor<Ingress> ingressCaptor = ArgumentCaptor.forClass(Ingress.class);
        when(mockIngressOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressOps.reconcile(anyString(), anyString(), ingressCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Ingress())));
        when(mockIngressOps.hasIngressAddress(eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForIngressTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_19), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(ingressCaptor.getAllValues().size(), is(4));
                    assertThat(ingressV1Beta1Captor.getAllValues().size(), is(0));

                    verify(mockIngressV1Beta1ops, never()).list(any(), any());
                    verify(mockIngressV1Beta1ops, never()).reconcile(any(), any(), any());
                    verify(mockIngressV1Beta1ops, never()).hasIngressAddress(any(), any(), anyLong(), anyLong());

                    async.flag();
                })));
    }

    /**
     * Override KafkaAssemblyOperator to only run reconciliation steps that concern the Ingress resources feature
     */
    class MockKafkaAssemblyOperatorForIngressTests extends KafkaAssemblyOperator {
        public MockKafkaAssemblyOperatorForIngressTests(
                Vertx vertx,
                PlatformFeaturesAvailability pfa,
                CertManager certManager,
                PasswordGenerator passwordGenerator,
                ResourceOperatorSupplier supplier,
                ClusterOperatorConfig config
        ) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.getKafkaClusterDescription()
                    .compose(state -> state.kafkaIngresses())
                    .compose(state -> state.kafkaIngressesV1Beta1())
                    .compose(state -> state.kafkaIngressesReady())
                    .compose(state -> state.kafkaIngressesV1Beta1Ready())
                    .map((Void) null);
        }
    }
}