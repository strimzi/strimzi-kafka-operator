/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
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
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertManagerUtils;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaExporterReconcilerCertManagerTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "name";
    private static final String USER_PROVIDED_CLUSTER_CA_SECRET_NAME = "my-cluster-ca-secret";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static OpenSslCertManager CERT_MANAGER = new OpenSslCertManager();
    private final static int VALIDITY_DAYS = 100;
    private final static int RENEWAL_DAYS = 10;
    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            NAME,
            ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), true),
            null,
            VALIDITY_DAYS,
            RENEWAL_DAYS,
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
                .withNewClusterCa()
                    .withGenerateCertificateAuthority(false)
                    .withType(CertificateManagerType.CERT_MANAGER_IO)
                    .withNewCertManager()
                        .withNewCaCert()
                            .withSecretName(USER_PROVIDED_CLUSTER_CA_SECRET_NAME)
                            .withCertificate(CA_CRT)
                        .endCaCert()
                        .withNewIssuerRef()
                            .withName("cm-issuer")
                            .withKind(IssuerKind.CLUSTER_ISSUER)
                        .endIssuerRef()
                    .endCertManager()
                .endClusterCa()
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

    private static CertificateAuthority getCertificateAuthority() {
        return new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .withNewCertManager()
                    .withNewCaCert()
                        .withSecretName("my-cluster-ca-secret")
                        .withCertificate(CA_CRT)
                    .endCaCert()
                    .withNewIssuerRef()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                    .endIssuerRef()
                .endCertManager()
                .build();
    }

    private CertAndKey generateCa(CertificateAuthority certificateAuthority) throws IOException {

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-ca").build();

        CERT_MANAGER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, ModelUtils.getCertificateValidity(certificateAuthority));
        return new CertAndKey(
                Files.readAllBytes(clusterCaKeyFile),
                Files.readAllBytes(clusterCaCertFile),
                null,
                null,
                null);
    }

    private CertAndKey renewClusterCaCert(CertAndKey certAndKey) throws IOException {
        Path caKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        caKeyFile.toFile().deleteOnExit();
        Files.write(caKeyFile, certAndKey.key());
        Path caCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        caCertFile.toFile().deleteOnExit();
        Files.write(caCertFile, certAndKey.cert());

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-ca").build();

        CERT_MANAGER.renewSelfSignedCert(caKeyFile.toFile(), caCertFile.toFile(), sbj, 10);

        return new CertAndKey(
                Files.readAllBytes(caKeyFile),
                Files.readAllBytes(caCertFile),
                null,
                null,
                null);
    }

    private Secret kafkaExporterCMSecret(CertAndKey ca) throws IOException {
        File csrFile = Files.createTempFile("tls", "csr").toFile();
        csrFile.deleteOnExit();
        File keyFile = Files.createTempFile("tls", "key").toFile();
        keyFile.deleteOnExit();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        certFile.deleteOnExit();

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-operator").build();

        CERT_MANAGER.generateCsr(keyFile, csrFile, sbj);
        CERT_MANAGER.generateCert(csrFile, ca.key(), ca.cert(), certFile, sbj, 10);

        CertAndKey clusterOperatorCertAndKey = new CertAndKey(
                Files.readAllBytes(keyFile.toPath()),
                Files.readAllBytes(certFile.toPath()),
                null,
                null,
                null);

        return createSecret(KafkaExporterResources.secretName(NAME) + "-cm",
                Map.of("tls.crt", clusterOperatorCertAndKey.certAsBase64String(),
                        "tls.key", clusterOperatorCertAndKey.keyAsBase64String()));
    }

    private Secret createSecret(String secretName, Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(data)
                .build();
    }

    /*
     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled. In such case, the KE Deployment and all other
     * resources should be created ot updated. So the reconcile methods should be called with non-null values.
     */
    @Test
    public void reconcileWithEnabledExporter(VertxTestContext context) throws IOException {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority();
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);
        // Create cert-manager Secret for Kafka Exporter cert as though Certificate request will succeed
        Secret kafkaExporterCMSecret = kafkaExporterCMSecret(clusterCaCertAndKey);

        initKafkaReconcilerTestMocks(supplier, kafkaExporterCMSecret);

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
                    // Certificate Object created
                    ArgumentCaptor<Certificate> kafkaExporterCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), kafkaExporterCertificate.capture());

                    CertificateSpec kafkaExporterCertificateSpec = kafkaExporterCertificate.getValue().getSpec();
                    assertThat(kafkaExporterCertificateSpec.getCommonName(), is(KafkaExporter.COMPONENT_TYPE));

                    ArgumentCaptor<Secret> kafkaExporterCertSecret = ArgumentCaptor.forClass(Secret.class);
                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), kafkaExporterCertSecret.capture());

                    // Kafka Exporter cert Secret is created
                    Map<String, String> kafkaExporterCertData = kafkaExporterCertSecret.getValue().getData();
                    assertThat(kafkaExporterCertData, aMapWithSize(2));
                    assertThat(kafkaExporterCertData.get("kafka-exporter.crt"), is(kafkaExporterCMSecret.getData().get("tls.crt")));
                    assertThat(kafkaExporterCertData.get("kafka-exporter.key"), is(kafkaExporterCMSecret.getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = kafkaExporterCertSecret.getValue().getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(kafkaExporterCMSecret, "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    async.flag();
                })));
    }
//
//    /*
//     * Tests Kafka Exporter reconciliation when Kafka Exporter is enabled. In such case, the KE Deployment and all other
//     * resources should be created ot updated. So the reconcile methods should be called with non-null values. However,
//     * when the network policy generation is disabled, network policies should not be touched (so the reconcile should
//     * not be called.)
//     */
//    @Test
//    public void reconcileWithEnabledExporterWithoutNetworkPolicies(VertxTestContext context) {
//        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
//
//        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
//        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
//        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        SecretOperator mockSecretOps = supplier.secretOperations;
//        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)))).thenReturn(Future.succeededFuture());
//        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
//        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
//        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());
//
//        DeploymentOperator mockDepOps = supplier.deploymentOperations;
//        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
//        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
//        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
//        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
//
//        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
//        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
//        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        Kafka kafka = new KafkaBuilder(KAFKA)
//                .editSpec()
//                    .withNewKafkaExporter()
//                    .endKafkaExporter()
//                .endSpec()
//                .build();
//
//        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
//                Reconciliation.DUMMY_RECONCILIATION,
//                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
//                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
//                supplier,
//                kafka,
//                VERSIONS,
//                CLUSTER_CA
//        );
//
//        Checkpoint async = context.checkpoint();
//        reconciler.reconcile(false, null, null, Clock.systemUTC())
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    assertThat(saCaptor.getAllValues().size(), is(1));
//                    assertThat(saCaptor.getValue(), is(notNullValue()));
//
//                    assertThat(secretCaptor.getAllValues().size(), is(1));
//                    assertThat(secretCaptor.getAllValues().get(0), is(notNullValue()));
//
//                    verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());
//
//                    assertThat(depCaptor.getAllValues().size(), is(1));
//                    assertThat(depCaptor.getValue(), is(notNullValue()));
//
//                    assertThat(pdbCaptor.getAllValues().size(), is(1));
//                    assertThat(pdbCaptor.getValue(), is(notNullValue()));
//                    assertThat(pdbCaptor.getValue().getSpec().getMinAvailable(), is(new IntOrString(0)));
//
//                    async.flag();
//                })));
//    }
//
//    /*
//     * Tests Kafka Exporter reconciliation when Kafka Exporter is disabled. In such case, the KE Deployment and all other
//     * resources should be deleted. So the reconcile methods should be called with null values.
//     */
//    @Test
//    public void reconcileWithDisabledExporter(VertxTestContext context) {
//        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
//
//        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
//        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
//        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        SecretOperator mockSecretOps = supplier.secretOperations;
//        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
//        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
//        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
//        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        DeploymentOperator mockDepOps = supplier.deploymentOperations;
//        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
//        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
//        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
//        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
//
//        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
//        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
//        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
//                Reconciliation.DUMMY_RECONCILIATION,
//                ResourceUtils.dummyClusterOperatorConfig(),
//                supplier,
//                KAFKA,
//                VERSIONS,
//                CLUSTER_CA
//        );
//
//        Checkpoint async = context.checkpoint();
//        reconciler.reconcile(false, null, null, Clock.systemUTC())
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    assertThat(saCaptor.getAllValues().size(), is(1));
//                    assertThat(saCaptor.getValue(), is(nullValue()));
//
//                    assertThat(secretCaptor.getAllValues().size(), is(1));
//                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));
//
//                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
//                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));
//
//                    assertThat(depCaptor.getAllValues().size(), is(1));
//                    assertThat(depCaptor.getValue(), is(nullValue()));
//
//                    assertThat(pdbCaptor.getAllValues().size(), is(1));
//                    assertThat(pdbCaptor.getValue(), is(nullValue()));
//
//                    async.flag();
//                })));
//    }
//
//    /*
//     * Tests Kafka Exporter reconciliation when Kafka Exporter is disabled. In such case, the KE Deployment and all other
//     * resources should be deleted. So the reconcile methods should be called with null values. However, when the network
//     * policy generation is disabled, network policies should not be touched (so the reconcile should not be called.)
//     */
//    @Test
//    public void reconcileWithDisabledExporterWithoutNetworkPolicies(VertxTestContext context) {
//        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
//
//        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
//        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
//        when(mockSaOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), saCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        SecretOperator mockSecretOps = supplier.secretOperations;
//        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
//        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.secretName(NAME)), secretCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
//        when(mockNetPolicyOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture());
//
//        DeploymentOperator mockDepOps = supplier.deploymentOperations;
//        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
//        when(mockDepOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), depCaptor.capture())).thenReturn(Future.succeededFuture());
//        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
//        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
//
//        PodDisruptionBudgetOperator mockPodDisruptionBudgetOps = supplier.podDisruptionBudgetOperator;
//        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
//        when(mockPodDisruptionBudgetOps.reconcile(any(), eq(NAMESPACE), eq(KafkaExporterResources.componentName(NAME)), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
//
//        KafkaExporterReconciler reconciler = new KafkaExporterReconciler(
//                Reconciliation.DUMMY_RECONCILIATION,
//                new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
//                        .with(ClusterOperatorConfig.NETWORK_POLICY_GENERATION.key(), "false").build(),
//                supplier,
//                KAFKA,
//                VERSIONS,
//                CLUSTER_CA
//        );
//
//        Checkpoint async = context.checkpoint();
//        reconciler.reconcile(false, null, null, Clock.systemUTC())
//                .onComplete(context.succeeding(v -> context.verify(() -> {
//                    assertThat(saCaptor.getAllValues().size(), is(1));
//                    assertThat(saCaptor.getValue(), is(nullValue()));
//
//                    assertThat(secretCaptor.getAllValues().size(), is(1));
//                    assertThat(secretCaptor.getAllValues().get(0), is(nullValue()));
//
//                    verify(mockNetPolicyOps, never()).reconcile(any(), eq(NAMESPACE), any(), any());
//
//                    assertThat(depCaptor.getAllValues().size(), is(1));
//                    assertThat(depCaptor.getValue(), is(nullValue()));
//
//                    assertThat(pdbCaptor.getAllValues().size(), is(1));
//                    assertThat(pdbCaptor.getValue(), is(nullValue()));
//
//                    async.flag();
//                })));
//    }

    private void initKafkaReconcilerTestMocks(ResourceOperatorSupplier supplier,
                                              Secret certManagerSecret) {
        SecretOperator secretOps = supplier.secretOperations;

        when(secretOps.getAsync(eq(NAMESPACE), eq(CertManagerUtils.certManagerSecretName(KafkaExporterResources.secretName(NAME))))).thenReturn(Future.succeededFuture(certManagerSecret));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        CertManagerCertificateOperator certManagerCertificateOperator = supplier.certManagerCertificateOperator;

        when(certManagerCertificateOperator.reconcile(any(), eq(NAMESPACE), any(), any(Certificate.class))).thenReturn(Future.succeededFuture());
        when(certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());
    }
}
