/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateSpec;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.common.certmanager.IssuerKind;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRefBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertManagerUtils;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaExporterReconcilerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new MockCertManager(),
            new PasswordGenerator(10, "a", "a"),
            NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey())
    );
    private final static ClusterCa CLUSTER_CA_WITH_CM = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            NAME,
            ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), true),
            null,
            100,
            10,
            false,
            CertificateManagerType.CERT_MANAGER_IO,
            null,
            new IssuerRefBuilder()
                    .withName("cm-issuer")
                    .withKind(IssuerKind.CLUSTER_ISSUER)
                    .build()
    );
    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(NAME)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                .endKafka()
            .endSpec()
            .build();

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled. In such case, the KE Deployment and all other
     * resources should be created ot updated. So the reconcile methods should be called with non-null values.
     */
    @Test
    public void reconcileWithEnabledExporter(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(notNullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().size(), is(1));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getPorts().size(), is(1));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(9404));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getFrom(), is(List.of()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is("4d715cdd"));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled. In such case, the KE Deployment and all other
     * resources should be created ot updated. So the reconcile methods should be called with non-null values. However,
     * when the network policy generation is disabled, network policies should not be touched (so the reconcile should
     * not be called.)
     */
    @Test
    public void reconcileWithEnabledExporterWithoutNetworkPolicies(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(notNullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(notNullValue()));

                    verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(notNullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled and cert-manager is used to issue certificates.
     * In such case, the KE Deployment and all other resources should be created ot updated. So the reconcile methods
     * should be called with non-null values.
     */
    @Test
    public void reconcileWithEnabledExporterWithCertManager(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        CertManagerCertificateOperator mockCertManagerOps = supplier.certManagerCertificateOperator;
        when(mockCertManagerOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), any(Certificate.class))).thenReturn(Future.succeededFuture());
        when(mockCertManagerOps.waitForReady(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)))).thenReturn(Future.succeededFuture());

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)))).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), any())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        // Create Strimzi and cert-manager created Secrets
        Secret strimziCertSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaExporterResources.secretName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "abc123"))
                .endMetadata()
                    .withData(Map.of("tls.crt", "cert", "tls.key", "key"))
                .build();
        Secret certManagerSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(CertManagerUtils.certManagerSecretName(KafkaExporterResources.secretName(NAME)))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "abc123"))
                .endMetadata()
                    .withData(Map.of("tls.crt", "cert", "tls.key", "key"))
                .build();

        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)))).thenReturn(Future.succeededFuture(strimziCertSecret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(CertManagerUtils.certManagerSecretName(KafkaExporterResources.secretName(NAME))))).thenReturn(Future.succeededFuture(certManagerSecret));
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), any(Secret.class))).thenReturn(Future.succeededFuture());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
                .endSpec()
                .build();

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                kafka,
                VERSIONS,
                CLUSTER_CA_WITH_CM
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Certificate Object reconciled
                    ArgumentCaptor<Certificate> certificateCaptor =  ArgumentCaptor.forClass(Certificate.class);
                    verify(mockCertManagerOps).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), certificateCaptor.capture());
                    CertificateSpec kafkaExporterCertificateSpec = certificateCaptor.getValue().getSpec();
                    assertThat(kafkaExporterCertificateSpec.getCommonName(), is(KafkaExporter.COMPONENT_TYPE));

                    // Strimzi managed cert Secret reconciled
                    ArgumentCaptor<Secret> certSecretCaptor = ArgumentCaptor.forClass(Secret.class);
                    verify(mockSecretOps).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), certSecretCaptor.capture());
                    Map<String, String> kafkaExporterCertData = certSecretCaptor.getValue().getData();
                    assertThat(kafkaExporterCertData, aMapWithSize(2));
                    assertThat(kafkaExporterCertData.get("kafka-exporter.crt"), is(certManagerSecret.getData().get("tls.crt")));
                    assertThat(kafkaExporterCertData.get("kafka-exporter.key"), is(certManagerSecret.getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = certSecretCaptor.getValue().getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, "abc123"));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

                    verify(mockSaOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), isNotNull());

                    ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
                    verify(mockNetPolicyOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), netPolicyCaptor.capture());
                    assertThat(netPolicyCaptor.getValue(), is(notNullValue()));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().size(), is(1));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getPorts().size(), is(1));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(9404));
                    assertThat(netPolicyCaptor.getValue().getSpec().getIngress().get(0).getFrom(), is(List.of()));

                    ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
                    verify(mockDepOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture());
                    assertThat(depCaptor.getValue(), is(notNullValue()));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION), is("0"));
                    assertThat(depCaptor.getValue().getSpec().getTemplate().getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is("4d715cdd"));


                    ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
                    verify(mockPodDisruptionBudgetOps, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture());
                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is disabled. In such case, the KE Deployment and all other
     * resources should be deleted. So the reconcile methods should be called with null values.
     */
    @Test
    public void reconcileWithDisabledExporter(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                KAFKA,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is disabled. In such case, the KE Deployment and all other
     * resources should be deleted. So the reconcile methods should be called with null values. However, when the network
     * policy generation is disabled, network policies should not be touched (so the reconcile should not be called.)
     */
    @Test
    public void reconcileWithDisabledExporterWithoutNetworkPolicies(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());

        SecretOperator mockSecretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());

        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
                Reconciliation.DUMMY_RECONCILIATION,
                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
                supplier,
                KAFKA,
                VERSIONS,
                CLUSTER_CA
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(false, null, null, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));

                    verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());

                    assertThat(depCaptor.getAllValues().size(), is(1));
                    assertThat(depCaptor.getValue(), is(nullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(nullValue()));

                    async.flag();
                })));
    }
}
