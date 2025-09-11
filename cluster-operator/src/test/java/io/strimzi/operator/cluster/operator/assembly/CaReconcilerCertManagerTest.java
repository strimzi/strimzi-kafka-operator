/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateSpec;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.common.certmanager.IssuerKind;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for actions taken by CaReconciler after Cert Manager provided CA Secrets are reconciled,
 * particularly the rolling updates for trust.
 * The test cases use a mock CaReconciler class to capture when Kafka pods and
 * other deployment (Kafka Exporter etc) are rolled.
 * <p>
 * Use CaReconcilerTest for testing the rolling updates when user managed, or Strimzi managed CA Secrets are used.
 * Use CaReconcilerReconcileCasTest for testing the reconcileCas method in isolation.
 */
@ExtendWith(VertxExtension.class)
public class CaReconcilerCertManagerTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-cluster";
    private static final String USER_PROVIDED_CLUSTER_CA_SECRET_NAME = "my-cluster-ca-secret";
    private static final String USER_PROVIDED_CLIENTS_CA_SECRET_NAME = "my-clients-ca-secret";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
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
                .withNewClientsCa()
                    .withGenerateCertificateAuthority(false)
                    .withType(CertificateManagerType.CERT_MANAGER_IO)
                    .withNewCertManager()
                        .withNewCaCert()
                            .withSecretName(USER_PROVIDED_CLIENTS_CA_SECRET_NAME)
                            .withCertificate(CA_CRT)
                        .endCaCert()
                        .withNewIssuerRef()
                            .withName("cm-issuer")
                            .withKind(IssuerKind.CLUSTER_ISSUER)
                        .endIssuerRef()
                    .endCertManager()
                .endClientsCa()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build();

    private final static OpenSslCertManager CERT_MANAGER = new OpenSslCertManager();
    private final static PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    private WorkerExecutor sharedWorkerExecutor;
    private ResourceOperatorSupplier supplier;

    @BeforeEach
    public void setup(Vertx vertx) {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        supplier = ResourceUtils.supplierWithMocks(false);
    }

    @AfterEach
    public void teardown() {
        sharedWorkerExecutor.close();
    }


    private CertAndKey generateCa(CertificateAuthority certificateAuthority, String commonName) throws IOException {

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();
        
        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(commonName).build();

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

    private Secret createSecret(String secretName, Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(data)
                .build();
    }

    private Secret clusterOperatorCMSecret(CertAndKey ca) throws IOException {
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

        return createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME) + "-cm",
                Map.of("tls.crt", clusterOperatorCertAndKey.certAsBase64String(),
                        "tls.key", clusterOperatorCertAndKey.keyAsBase64String()));
    }

    private Secret clusterOperatorSecret(Secret certManagerSecret, String certGeneration) {
        String certHash = CertUtils.getCertificateShortThumbprint(certManagerSecret, "tls.crt");
        Objects.requireNonNull(certHash);

        return ModelUtils.createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME), NAMESPACE, Labels.EMPTY, null,
                Map.of(
                        Ca.SecretEntry.CRT.asKey("cluster-operator"), certManagerSecret.getData().get("tls.crt"),
                        Ca.SecretEntry.KEY.asKey("cluster-operator"), certManagerSecret.getData().get("tls.key")
                ),
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, certGeneration, Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, certHash),
                Map.of());
    }

    @Test
    public void testClusterOperatorCertCreated(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, clusterCaCertAndKey.certAsBase64String()));
        Secret clusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), clusterCaCertAndKey.certAsBase64String(), true);
        // Create cert-manager Secret for cluster operator cert as though Certificate request will succeed
        Secret clusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret clientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                clusterOperatorCMSecret,
                null,
                List.of(clusterCaCertSecret, clientsCaCertSecret),
                List.of(),
                List.of());

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    // Kafka pods not rolled (they don't exist yet)
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    // Certificate object created
                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    ArgumentCaptor<Secret> clusterOperatorCertSecret = ArgumentCaptor.forClass(Secret.class);
                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertSecret.capture());

                    // Cluster Operator cert Secret is created
                    Map<String, String> clusterOperatorCertData = clusterOperatorCertSecret.getValue().getData();
                    assertThat(clusterOperatorCertData, aMapWithSize(2));
                    assertThat(clusterOperatorCertData.get("cluster-operator.crt"), is(clusterOperatorCMSecret.getData().get("tls.crt")));
                    assertThat(clusterOperatorCertData.get("cluster-operator.key"), is(clusterOperatorCMSecret.getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = clusterOperatorCertSecret.getValue().getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateShortThumbprint(clusterOperatorCMSecret, "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    async.flag();
                })));
    }

    @Test
    public void testClusterOperatorCertificateCreationFails(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, clusterCaCertAndKey.certAsBase64String()));
        Secret clusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), clusterCaCertAndKey.certAsBase64String(), true);

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                null,
                null,
                List.of(clusterCaCertSecret, initialClientsCaCertSecret),
                List.of(),
                List.of());

        when(supplier.certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME))))
                .thenReturn(Future.failedFuture(new TimeoutException("Timed out waiting for resource to be ready")));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.failing(throwable -> context.verify(() -> {
                    assertThat(throwable.getMessage(), is("Timed out waiting for resource to be ready"));

                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any());
                    async.flag();
                })));
    }

    @Test
    public void testClusterOperatorCertRenewed(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, clusterCaCertAndKey.certAsBase64String()));
        Secret clusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), clusterCaCertAndKey.certAsBase64String(), true);

        Secret initialClusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);
        Secret clusterOperatorSecret = clusterOperatorSecret(initialClusterOperatorCMSecret, "0");
        Secret renewedClusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret clientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        Pod controllerPod = podWithNameAndAnnotations("my-cluster-controllers-1", false, true, generationAnnotations);
        Pod brokerPod = podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                renewedClusterOperatorCMSecret,
                clusterOperatorSecret,
                List.of(clusterCaCertSecret, clientsCaCertSecret, clusterOperatorSecret),
                List.of(controllerPod),
                List.of(brokerPod));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    ArgumentCaptor<Secret> clusterOperatorCertSecret = ArgumentCaptor.forClass(Secret.class);
                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertSecret.capture());

                    Map<String, String> clusterOperatorCertData = clusterOperatorCertSecret.getValue().getData();
                    assertThat(clusterOperatorCertData, aMapWithSize(2));
                    assertThat(clusterOperatorCertData.get("cluster-operator.crt"), is(renewedClusterOperatorCMSecret.getData().get("tls.crt")));
                    assertThat(clusterOperatorCertData.get("cluster-operator.key"), is(renewedClusterOperatorCMSecret.getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = clusterOperatorCertSecret.getValue().getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateShortThumbprint(renewedClusterOperatorCMSecret, "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    async.flag();
                })));
    }

    @Test
    public void testCaAndClusterOperatorCertRenewed(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");
        CertAndKey renewedClusterCaCertAndKey = renewClusterCaCert(clusterCaCertAndKey);

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, renewedClusterCaCertAndKey.certAsBase64String()));
        Secret clusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), clusterCaCertAndKey.certAsBase64String(), true);

        Secret initialClusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);
        Secret clusterOperatorSecret = clusterOperatorSecret(initialClusterOperatorCMSecret, "0");
        Secret renewedClusterOperatorCMSecret = clusterOperatorCMSecret(renewedClusterCaCertAndKey);

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret clientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        Pod controllerPod = podWithNameAndAnnotations("my-cluster-controllers-1", false, true, generationAnnotations);
        Pod brokerPod = podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                renewedClusterOperatorCMSecret,
                clusterOperatorSecret,
                List.of(clusterCaCertSecret, clientsCaCertSecret, clusterOperatorSecret),
                List.of(controllerPod),
                List.of(brokerPod));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    ArgumentCaptor<Secret> clusterOperatorCertSecret = ArgumentCaptor.forClass(Secret.class);
                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertSecret.capture());

                    Map<String, String> clusterOperatorCertData = clusterOperatorCertSecret.getValue().getData();
                    assertThat(clusterOperatorCertData, aMapWithSize(2));
                    assertThat(clusterOperatorCertData.get("cluster-operator.crt"), is(renewedClusterOperatorCMSecret.getData().get("tls.crt")));
                    assertThat(clusterOperatorCertData.get("cluster-operator.key"), is(renewedClusterOperatorCMSecret.getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = clusterOperatorCertSecret.getValue().getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateShortThumbprint(renewedClusterOperatorCMSecret, "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
                    async.flag();
                })));
    }

    @Test
    public void testCaKeyReplacedAndClusterOperatorCertRenewed(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");
        CertAndKey newClusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, newClusterCaCertAndKey.certAsBase64String()));
        Secret clusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), clusterCaCertAndKey.certAsBase64String(), true);

        Secret initialClusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);
        Secret clusterOperatorSecret = clusterOperatorSecret(initialClusterOperatorCMSecret, "0");
        Secret renewedClusterOperatorCMSecret = clusterOperatorCMSecret(newClusterCaCertAndKey);

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret clientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        Pod controllerPod = podWithNameAndAnnotations("my-cluster-controllers-1", false, true, generationAnnotations);
        Pod brokerPod = podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                renewedClusterOperatorCMSecret,
                clusterOperatorSecret,
                List.of(clusterCaCertSecret, clientsCaCertSecret, clusterOperatorSecret),
                List.of(controllerPod),
                List.of(brokerPod));

        Checkpoint async = context.checkpoint(2);
        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class))).thenAnswer(i -> {
            List<StrimziPodSet> podSets = i.getArgument(2);
            context.verify(() -> {
                assertThat(podSets, hasSize(2));
                List<Pod> returnedPods = podSets
                        .stream()
                        .flatMap(podSet -> PodSetUtils.podSetToPods(podSet).stream())
                        .toList();
                for (Pod pod : returnedPods) {
                    Map<String, String> podAnnotations = pod.getMetadata().getAnnotations();
                    // Expect that the CA key generation was updated. CA cert generations are updated by component reconcilers
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                }
            });
            async.flag();
            return Future.succeededFuture();
        });

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(2));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
                    mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                            assertThat("Deployment restart reason for " + deploymentName, restartReason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true)));

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any());
                    async.flag();
                })));
    }

    // Strimzi Cluster CA key replaced and Cluster Operator cert renewed in previous reconcile and Kafka pods already rolled
    @Test
    public void testCaKeyReplacedAndClusterOperatorCertRenewedPreviously(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");
        CertAndKey newClusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, newClusterCaCertAndKey.certAsBase64String()));
        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), newClusterCaCertAndKey.certAsBase64String(), true);

        Map<String, String> annotations = initialClusterCaCertSecret.getMetadata().getAnnotations();
        annotations.put(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1");
        annotations.put(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1");
        Secret clusterCaCertSecret = initialClusterCaCertSecret.edit()
                .editMetadata()
                .withAnnotations(annotations)
                .endMetadata()
                .build();

        // add an "old" certificate to the secret with format ca-YYYY-MM-DDTHH-MM-SSZ
        // for ease re-use existing ca.crt file contents
        String oldCertAlias = "ca-2025-08-06T09-00-00Z.crt";
        clusterCaCertSecret.getData().put(oldCertAlias, initialClusterCaCertSecret.getData().get(CA_CRT));

        Secret initialClusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);
        Secret clusterOperatorSecret = clusterOperatorSecret(initialClusterOperatorCMSecret, "0");
        Secret renewedClusterOperatorCMSecret = clusterOperatorCMSecret(newClusterCaCertAndKey);

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret clientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        // Update annotations as though this is the second reconcile loop after Kafka brokers were rolled
        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        Pod controllerPod = podWithNameAndAnnotations("my-cluster-controllers-1", false, true, generationAnnotations);
        Pod brokerPod = podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                renewedClusterOperatorCMSecret,
                clusterOperatorSecret,
                List.of(clusterCaCertSecret, clientsCaCertSecret, clusterOperatorSecret),
                List.of(controllerPod),
                List.of(brokerPod));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    ArgumentCaptor<Secret> clusterOperatorCertSecret = ArgumentCaptor.forClass(Secret.class);
                    verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertSecret.capture());

                    Map<String, String> clusterOperatorCertData = clusterOperatorCertSecret.getValue().getData();
                    assertThat(clusterOperatorCertData, aMapWithSize(2));
                    assertThat(clusterOperatorCertData.get("cluster-operator.crt"), is(renewedClusterOperatorCMSecret.getData().get("tls.crt")));
                    assertThat(clusterOperatorCertData.get("cluster-operator.key"), is(renewedClusterOperatorCMSecret.getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = clusterOperatorCertSecret.getValue().getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateShortThumbprint(renewedClusterOperatorCMSecret, "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));

                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);

                    verify(supplier.secretOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
                    verify(supplier.secretOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());

                    //getValue() returns the latest captured value
                    Map<String, String> clusterCaCertData = clusterCaCert.getValue().getData();
                    assertThat(clusterCaCertData, aMapWithSize(2));
                    assertThat(clusterCaCertData.get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                    assertThat(clusterCaCertData.get(oldCertAlias), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                    async.flag();
                })));
    }

    @Test
    public void testOldClusterCaCertsGetsRemovedAuto(Vertx vertx, VertxTestContext context) throws IOException {
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLUSTER_CA_SECRET_NAME);
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority, "cluster-ca");

        Secret userProvidedClusterCaCertSecret = createSecret(USER_PROVIDED_CLUSTER_CA_SECRET_NAME, Map.of(CA_CRT, clusterCaCertAndKey.certAsBase64String()));
        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), clusterCaCertAndKey.certAsBase64String(), true);

        Map<String, String> annotations = initialClusterCaCertSecret.getMetadata().getAnnotations();
        annotations.put(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1");
        annotations.put(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1");
        Secret clusterCaCertSecret = initialClusterCaCertSecret.edit()
                .editMetadata()
                .withAnnotations(annotations)
                .endMetadata()
                .build();

        // add an "old" certificate to the secret with format ca-YYYY-MM-DDTHH-MM-SSZ
        // for ease re-use existing ca.crt file contents
        String oldCertAlias = "ca-2025-08-06T09-00-00Z.crt";
        clusterCaCertSecret.getData().put(oldCertAlias, initialClusterCaCertSecret.getData().get(CA_CRT));

        Secret clusterOperatorCMSecret = clusterOperatorCMSecret(clusterCaCertAndKey);
        Secret clusterOperatorSecret = clusterOperatorSecret(clusterOperatorCMSecret, "1");

        CertificateAuthority clientsCaCertificateAuthority = getCertificateAuthority(USER_PROVIDED_CLIENTS_CA_SECRET_NAME);
        CertAndKey clientsCaCertAndKey = generateCa(clientsCaCertificateAuthority, "clients-ca");

        Secret userProvidedClientsCaCertSecret = createSecret(USER_PROVIDED_CLIENTS_CA_SECRET_NAME, Map.of(CA_CRT, clientsCaCertAndKey.certAsBase64String()));
        Secret clientsCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), clientsCaCertAndKey.certAsBase64String(), false);

        // Update annotations as though this is the second reconcile loop after Kafka brokers were rolled, and they are presenting certificates that chain with new CA key
        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        Pod controllerPod = podWithNameAndAnnotations("my-cluster-controllers-1", false, true, generationAnnotations);
        Pod brokerPod = podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations);

        initTrustRolloutTestMocks(supplier,
                userProvidedClusterCaCertSecret,
                userProvidedClientsCaCertSecret,
                clusterOperatorCMSecret,
                clusterOperatorSecret,
                List.of(clusterCaCertSecret, clientsCaCertSecret, clusterOperatorSecret),
                List.of(controllerPod),
                List.of(brokerPod));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

                    ArgumentCaptor<Certificate> clusterOperatorCertificate =  ArgumentCaptor.forClass(Certificate.class);
                    verify(supplier.certManagerCertificateOperator).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), clusterOperatorCertificate.capture());

                    CertificateSpec clusterOperatorCertificateSpec = clusterOperatorCertificate.getValue().getSpec();
                    assertThat(clusterOperatorCertificateSpec.getCommonName(), is("cluster-operator"));

                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);

                    // Cluster CA should be reconciled twice, once initially, then when removing the old cert. Clients CA is only reconciled once
                    verify(supplier.secretOperations, times(2)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
                    verify(supplier.secretOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());

                    //getValue() returns the latest captured value
                    Map<String, String> clusterCaCertData = clusterCaCert.getValue().getData();
                    assertThat(clusterCaCertData, aMapWithSize(1));
                    assertThat(clusterCaCertData.get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));

                    async.flag();
                })));
    }

    private void initTrustRolloutTestMocks(ResourceOperatorSupplier supplier,
                                           Secret userClusterCaCertSecret,
                                           Secret userClientsCaCertSecret,
                                           Secret clusterOperatorCMSecret,
                                           Secret clusterOperatorSecret,
                                           List<Secret> secrets,
                                           List<Pod> controllerPods,
                                           List<Pod> brokerPods) {
        SecretOperator secretOps = supplier.secretOperations;

        when(secretOps.getAsync(eq(NAMESPACE), eq(USER_PROVIDED_CLUSTER_CA_SECRET_NAME))).thenReturn(Future.succeededFuture(userClusterCaCertSecret));
        when(secretOps.getAsync(eq(NAMESPACE), eq(USER_PROVIDED_CLIENTS_CA_SECRET_NAME))).thenReturn(Future.succeededFuture(userClientsCaCertSecret));
        when(secretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME) + "-cm"))).thenReturn(Future.succeededFuture(clusterOperatorCMSecret));
        when(secretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)))).thenReturn(Future.succeededFuture(clusterOperatorSecret));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(secrets));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        CertManagerCertificateOperator certManagerCertificateOperator = supplier.certManagerCertificateOperator;

        when(certManagerCertificateOperator.reconcile(any(), eq(NAMESPACE), any(), any(Certificate.class))).thenReturn(Future.succeededFuture());
        when(certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());

        PodOperator mockPodOps = supplier.podOperations;
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));
    }

    static class MockCaReconciler extends CaReconciler {
        Map<String, RestartReasons> kafkaRestartReasons = new HashMap<>();
        Map<String, String> deploymentRestartReasons = new HashMap<>();

        public MockCaReconciler(Kafka kafkaCr, ResourceOperatorSupplier supplier, Vertx vertx) {
            super(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                    kafkaCr,
                    new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                    supplier,
                    vertx,
                    CERT_MANAGER,
                    PASSWORD_GENERATOR
            );
        }

        @Override
        KafkaRoller createKafkaRoller(Set<NodeRef> nodes, TlsPemIdentity coTlsPemIdentity) {
            KafkaRoller mockKafkaRoller = mock(KafkaRoller.class);
            when(mockKafkaRoller.rollingRestart(any())).thenAnswer(i -> podOperator.listAsync(NAMESPACE, Labels.EMPTY)
                    .onSuccess(pods -> kafkaRestartReasons = pods.stream().collect(Collectors.toMap(
                            pod -> pod.getMetadata().getName(),
                            pod -> (RestartReasons) i.getArgument(0, Function.class).apply(pod))))
                    .mapEmpty());
            return mockKafkaRoller;
        }

        @Override
        Future<Void> rollDeploymentIfExists(String deploymentName, RestartReason reason) {
            return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            this.deploymentRestartReasons.put(deploymentName, reason.getDefaultNote());
                        }
                        return Future.succeededFuture();
                    });
        }
    }

    private static Pod podWithNameAndAnnotations(String name, boolean broker, boolean controller, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                    .withLabels(Map.of(
                            Labels.STRIMZI_CLUSTER_LABEL, NAME,
                            Labels.STRIMZI_CONTROLLER_ROLE_LABEL, Boolean.toString(controller),
                            Labels.STRIMZI_BROKER_ROLE_LABEL, Boolean.toString(broker)
                            ))
                .endMetadata()
                .build();
    }

    private static Deployment deploymentWithName(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }

    private static CertificateAuthority getCertificateAuthority(String userSecretName) {
        return new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .withNewCertManager()
                    .withNewCaCert()
                        .withSecretName(userSecretName)
                        .withCertificate(CA_CRT)
                    .endCaCert()
                    .withNewIssuerRef()
                        .withName("cm-issuer")
                        .withKind(IssuerKind.CLUSTER_ISSUER)
                    .endIssuerRef()
                .endCertManager()
                .build();
    }
}
