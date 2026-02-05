/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.common.certmanager.IssuerKind;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRefBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.CoreMatchers;
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
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerCertManagerTest {
    private final static String NAMESPACE = "test";
    private final static String NAME = "my-cluster";
    private static final String NODE_POOL_NAME = "mixed";
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();
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
    private final static ClientsCa CLIENTS_CA = new ClientsCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            KafkaResources.clientsCaCertificateSecretName(NAME),
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            KafkaResources.clientsCaKeySecretName(NAME),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), MockCertManager.clusterCaKey()),
            365,
            30,
            true,
            CertificateManagerType.STRIMZI_IO,
            null);
    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
    private static final KafkaNodePool KAFKA_NODE_POOL = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName(NODE_POOL_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME))
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withDeleteClaim(true).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static OpenSslCertManager CERT_MANAGER = new OpenSslCertManager();

    private WorkerExecutor sharedWorkerExecutor;
    private ResourceOperatorSupplier supplier;

    @BeforeEach
    public void setup(Vertx vertx) throws IOException {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        supplier = ResourceUtils.supplierWithMocks(false);
    }

    @AfterEach
    public void teardown()    {
        sharedWorkerExecutor.close();
    }

    private static ClusterCa createClusterCaWithGeneration1() {
        Secret clusterCaCertSecret = ResourceUtils.createInitialCaCertSecretForCMCa(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), MockCertManager.clusterCaCert(), true);
        Map<String, String> annotations = clusterCaCertSecret.getMetadata().getAnnotations();
        annotations.put(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1");
        return new ClusterCa(
                Reconciliation.DUMMY_RECONCILIATION,
                new OpenSslCertManager(),
                new PasswordGenerator(10, "a", "a"),
                NAME,
                clusterCaCertSecret.edit()
                        .editMetadata()
                            .withAnnotations(annotations)
                        .endMetadata()
                        .build(),
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

    private Secret kafkaNodeCMSecret(String podName, CertAndKey ca) throws IOException {
        File csrFile = Files.createTempFile("tls", "csr").toFile();
        csrFile.deleteOnExit();
        File keyFile = Files.createTempFile("tls", "key").toFile();
        keyFile.deleteOnExit();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        certFile.deleteOnExit();

        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(KafkaResources.kafkaComponentName(NAME)).build();

        CERT_MANAGER.generateCsr(keyFile, csrFile, sbj);
        CERT_MANAGER.generateCert(csrFile, ca.key(), ca.cert(), certFile, sbj, 10);

        CertAndKey kafkaNodeCertAndKey = new CertAndKey(
                Files.readAllBytes(keyFile.toPath()),
                Files.readAllBytes(certFile.toPath()),
                null,
                null,
                null);

        return createSecret(podName + "-cm",
                Map.of("tls.crt", kafkaNodeCertAndKey.certAsBase64String(),
                        "tls.key", kafkaNodeCertAndKey.keyAsBase64String()));
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

    private Secret kafkaNodeCertSecret(String podName, Secret certManagerSecret) {
        String certHash = CertUtils.getCertificateThumbprint(certManagerSecret, "tls.crt");
        Objects.requireNonNull(certHash);

        return ModelUtils.createSecret(podName, NAMESPACE, Labels.EMPTY, null,
                Map.of(
                        Ca.SecretEntry.CRT.asKey(podName), certManagerSecret.getData().get("tls.crt"),
                        Ca.SecretEntry.KEY.asKey(podName), certManagerSecret.getData().get("tls.key")
                ),
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0", Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, certHash),
                Map.of());
    }

    @Test
    public void testCreateCertificateResources(Vertx vertx, VertxTestContext context) throws IOException {
        List<String> kafkaPodNames = List.of(NAME + "-" + NODE_POOL_NAME + "-0",
                NAME + "-" + NODE_POOL_NAME + "-1",
                NAME + "-" + NODE_POOL_NAME + "-2");
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority();
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);
        // Create cert-manager Secrets for Kafka node certs as though Certificate request will succeed
        Map<String, Secret> kafkaNodeCMSecrets = kafkaPodNames.stream()
                .map(podName -> {
                    Secret cmSecret = null;
                    try {
                        cmSecret = kafkaNodeCMSecret(podName, clusterCaCertAndKey);
                    } catch (IOException e) {
                        context.failNow(e);
                    }
                    return cmSecret;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

        initKafkaReconcilerTestMocks(supplier, new ArrayList<>(kafkaNodeCMSecrets.values()));

        Checkpoint async = context.checkpoint();
        KafkaReconciler reconciler = new MockKafkaReconcilerCertManagerTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                supplier,
                vertx,
                KAFKA,
                List.of(KAFKA_NODE_POOL),
                Map.of(Ca.SecretEntry.CRT.asKey("ca"), clusterCaCertAndKey.certAsBase64String()));
        reconciler.reconcile(new KafkaStatus(), Clock.systemUTC()).onComplete(context.succeeding(v -> context.verify(() -> {
            // Certificate Objects created
            ArgumentCaptor<Certificate> kafkaNodeCertificate =  ArgumentCaptor.forClass(Certificate.class);
            verify(supplier.certManagerCertificateOperator, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertificate.capture());

            List<String> certificates = kafkaNodeCertificate.getAllValues()
                    .stream()
                    .map(certificate -> certificate.getMetadata().getName())
                    .toList();

            assertThat(certificates, hasSize(3));
            assertThat(certificates, containsInAnyOrder(kafkaPodNames.toArray()));
            kafkaNodeCertificate.getAllValues().forEach(certificate -> assertThat(certificate.getSpec().getCommonName(), is(KafkaResources.kafkaComponentName(NAME))));

            // Kafka node cert Secrets are created
            ArgumentCaptor<Secret> kafkaNodeCertSecrets = ArgumentCaptor.forClass(Secret.class);
            verify(supplier.secretOperations, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertSecrets.capture());

            Map<String, Secret> certSecrets = kafkaNodeCertSecrets.getAllValues()
                    .stream()
                    .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(certSecrets.keySet(), hasSize(3));
            assertThat(certSecrets.keySet(), containsInAnyOrder(kafkaPodNames.toArray()));

            certSecrets.values().forEach(secret -> {
                String secretName = secret.getMetadata().getName();
                String cMSecretName = secretName + "-cm";
                Map<String, String> kafkaNodeCertData = secret.getData();
                assertThat(kafkaNodeCertData, aMapWithSize(2));
                assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(kafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(kafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(kafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
            });
            async.flag();
        })));
    }

    @Test
    public void testKafkaNodeCertificateCreationFails(Vertx vertx, VertxTestContext context) throws IOException {
        List<String> kafkaPodNames = List.of(NAME + "-" + NODE_POOL_NAME + "-0",
                NAME + "-" + NODE_POOL_NAME + "-1",
                NAME + "-" + NODE_POOL_NAME + "-2");

        initKafkaReconcilerTestMocks(supplier, List.of());

        when(supplier.certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME)))
                .thenReturn(Future.failedFuture(new TimeoutException("Timed out waiting for resource to be ready")));

        Checkpoint async = context.checkpoint();
        KafkaReconciler reconciler = new MockKafkaReconcilerCertManagerTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                supplier,
                vertx,
                KAFKA,
                List.of(KAFKA_NODE_POOL),
                null);
        reconciler.reconcile(new KafkaStatus(), Clock.systemUTC()).onComplete(context.failing(throwable -> context.verify(() -> {
            assertThat(throwable.getMessage(), CoreMatchers.is("Timed out waiting for resource to be ready"));

            // Certificate Objects created
            ArgumentCaptor<Certificate> kafkaNodeCertificate =  ArgumentCaptor.forClass(Certificate.class);
            verify(supplier.certManagerCertificateOperator, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertificate.capture());

            List<String> certificates = kafkaNodeCertificate.getAllValues()
                    .stream()
                    .map(certificate -> certificate.getMetadata().getName())
                    .toList();

            assertThat(certificates, hasSize(3));
            assertThat(certificates, containsInAnyOrder(kafkaPodNames.toArray()));
            kafkaNodeCertificate.getAllValues().forEach(certificate -> assertThat(certificate.getSpec().getCommonName(), is(KafkaResources.kafkaComponentName(NAME))));

            // Kafka node cert Secrets are not created
            verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), any());
            async.flag();
        })));
    }

    @Test
    public void testKafkaNodeCertRenewed(Vertx vertx, VertxTestContext context) throws IOException {
        List<String> kafkaPodNames = List.of(NAME + "-" + NODE_POOL_NAME + "-0",
                NAME + "-" + NODE_POOL_NAME + "-1",
                NAME + "-" + NODE_POOL_NAME + "-2");
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority();
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);

        Map<String, Secret> initialKafkaNodeCMSecrets = new HashMap<>();
        Map<String, Secret> kafkaNodeCertSecrets = new HashMap<>();
        Map<String, Secret> renewedKafkaNodeCMSecrets = new HashMap<>();

        kafkaPodNames.forEach(podName -> {
            try {
                Secret initialCmSecret = kafkaNodeCMSecret(podName, clusterCaCertAndKey);
                Secret renewedCmSecret = kafkaNodeCMSecret(podName, clusterCaCertAndKey);
                Secret kafkaNodeCertSecret = kafkaNodeCertSecret(podName, initialCmSecret);

                initialKafkaNodeCMSecrets.put(initialCmSecret.getMetadata().getName(), initialCmSecret);
                kafkaNodeCertSecrets.put(kafkaNodeCertSecret.getMetadata().getName(), kafkaNodeCertSecret);
                renewedKafkaNodeCMSecrets.put(renewedCmSecret.getMetadata().getName(), renewedCmSecret);
            } catch (IOException e) {
                context.failNow(e);
            }
        });

        // Use the renewed certs for 0 and 2, but leave the initial cert for 2
        List<Secret> secrets = new ArrayList<>(kafkaNodeCertSecrets.values());
        secrets.add(initialKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-1-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-0-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-2-cm"));

        initKafkaReconcilerTestMocks(supplier, secrets);

        Checkpoint async = context.checkpoint();
        KafkaReconciler reconciler = new MockKafkaReconcilerCertManagerTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                supplier,
                vertx,
                KAFKA,
                List.of(KAFKA_NODE_POOL),
                Map.of(Ca.SecretEntry.CRT.asKey("ca"), clusterCaCertAndKey.certAsBase64String()));
        reconciler.reconcile(new KafkaStatus(), Clock.systemUTC()).onComplete(context.succeeding(v -> context.verify(() -> {
            // Certificate Objects created
            ArgumentCaptor<Certificate> kafkaNodeCertificate =  ArgumentCaptor.forClass(Certificate.class);
            verify(supplier.certManagerCertificateOperator, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertificate.capture());

            List<String> certificates = kafkaNodeCertificate.getAllValues()
                    .stream()
                    .map(certificate -> certificate.getMetadata().getName())
                    .toList();

            assertThat(certificates, hasSize(3));
            assertThat(certificates, containsInAnyOrder(kafkaPodNames.toArray()));
            kafkaNodeCertificate.getAllValues().forEach(certificate -> assertThat(certificate.getSpec().getCommonName(), is(KafkaResources.kafkaComponentName(NAME))));

            // Kafka node cert Secrets are created
            ArgumentCaptor<Secret> capturedKafkaNodeCertSecrets = ArgumentCaptor.forClass(Secret.class);
            verify(supplier.secretOperations, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), capturedKafkaNodeCertSecrets.capture());

            Map<String, Secret> certSecrets = capturedKafkaNodeCertSecrets.getAllValues()
                    .stream()
                    .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(certSecrets.keySet(), hasSize(3));
            assertThat(certSecrets.keySet(), containsInAnyOrder(kafkaPodNames.toArray()));

            certSecrets.values().forEach(secret -> {
                String secretName = secret.getMetadata().getName();
                String cMSecretName = secretName + "-cm";
                Map<String, String> kafkaNodeCertData = secret.getData();
                if (secretName.endsWith("-1")) {
                    assertThat(kafkaNodeCertData, aMapWithSize(2));
                    assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                    assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(initialKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                } else {
                    assertThat(kafkaNodeCertData, aMapWithSize(2));
                    assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(renewedKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                    assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(renewedKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(renewedKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                }
            });
            async.flag();
        })));
    }

    @Test
    public void testClusterCaAndKafkaNodeCertRenewed(Vertx vertx, VertxTestContext context) throws IOException {
        List<String> kafkaPodNames = List.of(NAME + "-" + NODE_POOL_NAME + "-0",
                NAME + "-" + NODE_POOL_NAME + "-1",
                NAME + "-" + NODE_POOL_NAME + "-2");
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority();
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);
        CertAndKey renewedClusterCaCertAndKey = renewClusterCaCert(clusterCaCertAndKey);

        Map<String, Secret> initialKafkaNodeCMSecrets = new HashMap<>();
        Map<String, Secret> kafkaNodeCertSecrets = new HashMap<>();
        Map<String, Secret> renewedKafkaNodeCMSecrets = new HashMap<>();

        kafkaPodNames.forEach(podName -> {
            try {
                Secret initialCmSecret = kafkaNodeCMSecret(podName, clusterCaCertAndKey);
                Secret renewedCmSecret = kafkaNodeCMSecret(podName, renewedClusterCaCertAndKey);
                Secret kafkaNodeCertSecret = kafkaNodeCertSecret(podName, initialCmSecret);

                initialKafkaNodeCMSecrets.put(initialCmSecret.getMetadata().getName(), initialCmSecret);
                kafkaNodeCertSecrets.put(kafkaNodeCertSecret.getMetadata().getName(), kafkaNodeCertSecret);
                renewedKafkaNodeCMSecrets.put(renewedCmSecret.getMetadata().getName(), renewedCmSecret);
            } catch (IOException e) {
                context.failNow(e);
            }
        });

        // Use the renewed certs for 0 and 2, but leave the initial cert for 2
        List<Secret> secrets = new ArrayList<>(kafkaNodeCertSecrets.values());
        secrets.add(initialKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-1-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-0-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-2-cm"));

        initKafkaReconcilerTestMocks(supplier, secrets);

        Checkpoint async = context.checkpoint();
        KafkaReconciler reconciler = new MockKafkaReconcilerCertManagerTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                supplier,
                vertx,
                KAFKA,
                List.of(KAFKA_NODE_POOL),
                Map.of(Ca.SecretEntry.CRT.asKey("ca"), clusterCaCertAndKey.certAsBase64String()),
                createClusterCaWithGeneration1());
        reconciler.reconcile(new KafkaStatus(), Clock.systemUTC()).onComplete(context.succeeding(v -> context.verify(() -> {
            // Certificate Objects created
            ArgumentCaptor<Certificate> kafkaNodeCertificate =  ArgumentCaptor.forClass(Certificate.class);
            verify(supplier.certManagerCertificateOperator, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertificate.capture());

            List<String> certificates = kafkaNodeCertificate.getAllValues()
                    .stream()
                    .map(certificate -> certificate.getMetadata().getName())
                    .toList();

            assertThat(certificates, hasSize(3));
            assertThat(certificates, containsInAnyOrder(kafkaPodNames.toArray()));
            kafkaNodeCertificate.getAllValues().forEach(certificate -> assertThat(certificate.getSpec().getCommonName(), is(KafkaResources.kafkaComponentName(NAME))));

            // Kafka node cert Secrets are created
            ArgumentCaptor<Secret> capturedKafkaNodeCertSecrets = ArgumentCaptor.forClass(Secret.class);
            verify(supplier.secretOperations, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), capturedKafkaNodeCertSecrets.capture());

            Map<String, Secret> certSecrets = capturedKafkaNodeCertSecrets.getAllValues()
                    .stream()
                    .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(certSecrets.keySet(), hasSize(3));
            assertThat(certSecrets.keySet(), containsInAnyOrder(kafkaPodNames.toArray()));

            certSecrets.values().forEach(secret -> {
                String secretName = secret.getMetadata().getName();
                String cMSecretName = secretName + "-cm";
                Map<String, String> kafkaNodeCertData = secret.getData();
                if (secretName.endsWith("-1")) {
                    assertThat(kafkaNodeCertData, aMapWithSize(2));
                    assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                    assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(initialKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                } else {
                    assertThat(kafkaNodeCertData, aMapWithSize(2));
                    assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(renewedKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                    assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(renewedKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(renewedKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
                }
            });
            async.flag();
        })));
    }

    @Test
    public void testClusterCaKeyReplacedAndKafkaNodeCertRenewed(Vertx vertx, VertxTestContext context) throws IOException {
        List<String> kafkaPodNames = List.of(NAME + "-" + NODE_POOL_NAME + "-0",
                NAME + "-" + NODE_POOL_NAME + "-1",
                NAME + "-" + NODE_POOL_NAME + "-2");
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority();
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);
        CertAndKey newClusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);

        Map<String, Secret> initialKafkaNodeCMSecrets = new HashMap<>();
        Map<String, Secret> kafkaNodeCertSecrets = new HashMap<>();
        Map<String, Secret> renewedKafkaNodeCMSecrets = new HashMap<>();

        kafkaPodNames.forEach(podName -> {
            try {
                Secret initialCmSecret = kafkaNodeCMSecret(podName, clusterCaCertAndKey);
                Secret renewedCmSecret = kafkaNodeCMSecret(podName, newClusterCaCertAndKey);
                Secret kafkaNodeCertSecret = kafkaNodeCertSecret(podName, initialCmSecret);

                initialKafkaNodeCMSecrets.put(initialCmSecret.getMetadata().getName(), initialCmSecret);
                kafkaNodeCertSecrets.put(kafkaNodeCertSecret.getMetadata().getName(), kafkaNodeCertSecret);
                renewedKafkaNodeCMSecrets.put(renewedCmSecret.getMetadata().getName(), renewedCmSecret);
            } catch (IOException e) {
                context.failNow(e);
            }
        });

        // Use the renewed certs for 0 and 2, but leave the initial cert for 2
        List<Secret> secrets = new ArrayList<>(kafkaNodeCertSecrets.values());
        secrets.add(initialKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-1-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-0-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-2-cm"));

        initKafkaReconcilerTestMocks(supplier, secrets);

        Checkpoint async = context.checkpoint();
        KafkaReconciler reconciler = new MockKafkaReconcilerCertManagerTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                supplier,
                vertx,
                KAFKA,
                List.of(KAFKA_NODE_POOL),
                Map.of(Ca.SecretEntry.CRT.asKey("ca"), clusterCaCertAndKey.certAsBase64String(),
                        Ca.SecretEntry.CRT.asKey("ca-2025-12-15T09-00-00Z.crt"), newClusterCaCertAndKey.certAsBase64String()),
                createClusterCaWithGeneration1());
        reconciler.reconcile(new KafkaStatus(), Clock.systemUTC()).onComplete(context.succeeding(v -> context.verify(() -> {
            // Certificate Objects created
            ArgumentCaptor<Certificate> kafkaNodeCertificate =  ArgumentCaptor.forClass(Certificate.class);
            verify(supplier.certManagerCertificateOperator, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertificate.capture());

            List<String> certificates = kafkaNodeCertificate.getAllValues()
                    .stream()
                    .map(certificate -> certificate.getMetadata().getName())
                    .toList();

            assertThat(certificates, hasSize(3));
            assertThat(certificates, containsInAnyOrder(kafkaPodNames.toArray()));
            kafkaNodeCertificate.getAllValues().forEach(certificate -> assertThat(certificate.getSpec().getCommonName(), is(KafkaResources.kafkaComponentName(NAME))));

            // Kafka node cert Secrets are created
            ArgumentCaptor<Secret> capturedKafkaNodeCertSecrets = ArgumentCaptor.forClass(Secret.class);
            verify(supplier.secretOperations, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), capturedKafkaNodeCertSecrets.capture());

            Map<String, Secret> certSecrets = capturedKafkaNodeCertSecrets.getAllValues()
                    .stream()
                    .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(certSecrets.keySet(), hasSize(3));
            assertThat(certSecrets.keySet(), containsInAnyOrder(kafkaPodNames.toArray()));

            certSecrets.values().forEach(secret -> {
                String secretName = secret.getMetadata().getName();
                String cMSecretName = secretName + "-cm";
                Map<String, String> kafkaNodeCertData = secret.getData();
                if (secretName.endsWith("-1")) {
                    assertThat(kafkaNodeCertData, aMapWithSize(2));
                    assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                    assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(initialKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                } else {
                    assertThat(kafkaNodeCertData, aMapWithSize(2));
                    assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(renewedKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                    assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(renewedKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                    Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(renewedKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                    assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
                }
            });
            async.flag();
        })));
    }

    @Test
    public void testKafkaNodeCertRenewedAndNotTrusted(Vertx vertx, VertxTestContext context) throws IOException {
        List<String> kafkaPodNames = List.of(NAME + "-" + NODE_POOL_NAME + "-0",
                NAME + "-" + NODE_POOL_NAME + "-1",
                NAME + "-" + NODE_POOL_NAME + "-2");
        CertificateAuthority clusterCaCertificateAuthority = getCertificateAuthority();
        CertAndKey clusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);
        CertAndKey newClusterCaCertAndKey = generateCa(clusterCaCertificateAuthority);

        Map<String, Secret> initialKafkaNodeCMSecrets = new HashMap<>();
        Map<String, Secret> kafkaNodeCertSecrets = new HashMap<>();
        Map<String, Secret> renewedKafkaNodeCMSecrets = new HashMap<>();

        kafkaPodNames.forEach(podName -> {
            try {
                Secret initialCmSecret = kafkaNodeCMSecret(podName, clusterCaCertAndKey);
                Secret renewedCmSecret = kafkaNodeCMSecret(podName, newClusterCaCertAndKey);
                Secret kafkaNodeCertSecret = kafkaNodeCertSecret(podName, initialCmSecret);

                initialKafkaNodeCMSecrets.put(initialCmSecret.getMetadata().getName(), initialCmSecret);
                kafkaNodeCertSecrets.put(kafkaNodeCertSecret.getMetadata().getName(), kafkaNodeCertSecret);
                renewedKafkaNodeCMSecrets.put(renewedCmSecret.getMetadata().getName(), renewedCmSecret);
            } catch (IOException e) {
                context.failNow(e);
            }
        });

        // Use the renewed certs for 0 and 2, but leave the initial cert for 2
        List<Secret> secrets = new ArrayList<>(kafkaNodeCertSecrets.values());
        secrets.add(initialKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-1-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-0-cm"));
        secrets.add(renewedKafkaNodeCMSecrets.get(NAME + "-" + NODE_POOL_NAME + "-2-cm"));

        initKafkaReconcilerTestMocks(supplier, secrets);

        Checkpoint async = context.checkpoint();
        KafkaReconciler reconciler = new MockKafkaReconcilerCertManagerTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                supplier,
                vertx,
                KAFKA,
                List.of(KAFKA_NODE_POOL),
                Map.of(Ca.SecretEntry.CRT.asKey("ca"), clusterCaCertAndKey.certAsBase64String()),
                createClusterCaWithGeneration1());
        reconciler.reconcile(new KafkaStatus(), Clock.systemUTC()).onComplete(context.succeeding(v -> context.verify(() -> {
            // Certificate Objects created
            ArgumentCaptor<Certificate> kafkaNodeCertificate =  ArgumentCaptor.forClass(Certificate.class);
            verify(supplier.certManagerCertificateOperator, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), kafkaNodeCertificate.capture());

            List<String> certificates = kafkaNodeCertificate.getAllValues()
                    .stream()
                    .map(certificate -> certificate.getMetadata().getName())
                    .toList();

            assertThat(certificates, hasSize(3));
            assertThat(certificates, containsInAnyOrder(kafkaPodNames.toArray()));
            kafkaNodeCertificate.getAllValues().forEach(certificate -> assertThat(certificate.getSpec().getCommonName(), is(KafkaResources.kafkaComponentName(NAME))));

            // Kafka node cert Secrets are created
            ArgumentCaptor<Secret> capturedKafkaNodeCertSecrets = ArgumentCaptor.forClass(Secret.class);
            verify(supplier.secretOperations, times(3)).reconcile(any(), eq(NAMESPACE), startsWith(NAME + "-" + NODE_POOL_NAME), capturedKafkaNodeCertSecrets.capture());

            Map<String, Secret> certSecrets = capturedKafkaNodeCertSecrets.getAllValues()
                    .stream()
                    .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(certSecrets.keySet(), hasSize(3));
            assertThat(certSecrets.keySet(), containsInAnyOrder(kafkaPodNames.toArray()));

            certSecrets.values().forEach(secret -> {
                String secretName = secret.getMetadata().getName();
                String cMSecretName = secretName + "-cm";
                Map<String, String> kafkaNodeCertData = secret.getData();
                //Since certs are not trusted, certs should not be updated
                assertThat(kafkaNodeCertData, aMapWithSize(2));
                assertThat(kafkaNodeCertData.get(secretName + ".crt"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.crt")));
                assertThat(kafkaNodeCertData.get(secretName + ".key"), CoreMatchers.is(initialKafkaNodeCMSecrets.get(cMSecretName).getData().get("tls.key")));

                Map<String, String> clusterOperatorCertSecretAnnotations = secret.getMetadata().getAnnotations();
                assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, CertUtils.getCertificateThumbprint(initialKafkaNodeCMSecrets.get(cMSecretName), "tls.crt")));
                assertThat(clusterOperatorCertSecretAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
            });
            async.flag();
        })));
    }

    private void initKafkaReconcilerTestMocks(ResourceOperatorSupplier supplier,
                                           List<Secret> secrets) {
        SecretOperator secretOps = supplier.secretOperations;

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(secrets));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        CertManagerCertificateOperator certManagerCertificateOperator = supplier.certManagerCertificateOperator;

        when(certManagerCertificateOperator.reconcile(any(), eq(NAMESPACE), any(), any(Certificate.class))).thenReturn(Future.succeededFuture());
        when(certManagerCertificateOperator.waitForReady(any(), eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());
    }

    static class MockKafkaReconcilerCertManagerTasks extends KafkaReconciler {
        private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MockKafkaReconcilerCertManagerTasks.class.getName());
        private final Map<String, String> clusterCaCertData;

        public MockKafkaReconcilerCertManagerTasks(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Vertx vertx, Kafka kafkaCr, List<KafkaNodePool> kafkaNodePools, Map<String, String> clusterCaCertData) {
            super(reconciliation, kafkaCr, null, createKafkaCluster(reconciliation, supplier, kafkaCr, kafkaNodePools), CLUSTER_CA, CLIENTS_CA, CO_CONFIG, supplier, PFA, vertx);
            this.clusterCaCertData = clusterCaCertData;
        }

        public MockKafkaReconcilerCertManagerTasks(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Vertx vertx, Kafka kafkaCr, List<KafkaNodePool> kafkaNodePools, Map<String, String> clusterCaCertData, ClusterCa clusterCa) {
            super(reconciliation, kafkaCr, null, createKafkaCluster(reconciliation, supplier, kafkaCr, kafkaNodePools), clusterCa, CLIENTS_CA, CO_CONFIG, supplier, PFA, vertx);
            this.clusterCaCertData = clusterCaCertData;
        }

        private static KafkaCluster createKafkaCluster(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr, List<KafkaNodePool> kafkaNodePools)   {
            return  KafkaClusterCreator.createKafkaCluster(
                    reconciliation,
                    kafkaCr,
                    kafkaNodePools,
                    Map.of(),
                    KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                    VERSIONS,
                    supplier.sharedEnvironmentProvider);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return initClientAuthenticationCertificates()
                    .compose(i -> listeners())
                    .compose(i -> maybeReconcileCertManagerCertificates())
                    .compose(i -> certificateSecrets(clock))
                    .recover(error -> {
                        LOGGER.errorCr(reconciliation, "Reconciliation failed", error);
                        return Future.failedFuture(error);
                    });
        }

        @Override
        protected Future<Void> initClientAuthenticationCertificates() {
            if (clusterCaCertData != null) {
                Secret clusterCaCert = new SecretBuilder()
                        .withNewMetadata()
                        .withName(NAME + "cluster-ca-cert")
                        .endMetadata()
                        .withData(clusterCaCertData)
                        .build();
                coTlsPemIdentity = new TlsPemIdentity(new PemTrustSet(clusterCaCert), null);
            } else {
                coTlsPemIdentity = TlsPemIdentity.DUMMY_IDENTITY;
            }
            return Future.succeededFuture();
        }

        @Override
        protected Future<Void> listeners()  {
            listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
            listenerReconciliationResults.bootstrapNodePorts.put("external-9094", 31234);
            listenerReconciliationResults.listenerStatuses.add(new ListenerStatusBuilder().withName("external").build());

            return Future.succeededFuture();
        }
    }
}
