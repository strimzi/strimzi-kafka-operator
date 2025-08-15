/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static io.strimzi.operator.common.model.Ca.CA_KEY;
import static io.strimzi.operator.common.model.Ca.CA_STORE;
import static io.strimzi.operator.common.model.Ca.CA_STORE_PASSWORD;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CaReconcilerReconcileCasTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
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

    private final List<Secret> secrets = new ArrayList<>();
    private WorkerExecutor sharedWorkerExecutor;
    private ResourceOperatorSupplier supplier;

    @BeforeEach
    public void setup(Vertx vertx) {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        supplier = ResourceUtils.supplierWithMocks(false);
    }

    @AfterEach
    public void teardown() {
        secrets.clear();
        sharedWorkerExecutor.close();
    }

    private Future<Void> reconcileCas(Vertx vertx, CertificateAuthority clusterCa, CertificateAuthority clientsCa) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(clusterCa)
                    .withClientsCa(clientsCa)
                .endSpec()
                .build();

        return reconcileCas(vertx, kafka, Clock.systemUTC());
    }

    private Future<Void> reconcileCas(Vertx vertx, Kafka kafka, Clock clock) {
        SecretOperator secretOps = supplier.secretOperations;

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenAnswer(invocation -> {
            Map<String, String> requiredLabels = ((Labels) invocation.getArgument(1)).toMap();

            List<Secret> listedSecrets = secrets.stream().filter(s -> {
                Map<String, String> labels = s.getMetadata().getLabels();
                labels.keySet().retainAll(requiredLabels.keySet());
                return labels.equals(requiredLabels);
            }).collect(Collectors.toList());

            return Future.succeededFuture(listedSecrets);
        });

        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Promise<Void> reconcileCasComplete = Promise.promise();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR)
                .reconcileCas(clock)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        reconcileCasComplete.complete();
                    } else {
                        reconcileCasComplete.fail(ar.cause());
                    }
                });

        return reconcileCasComplete.future();
    }

    private CertAndKey generateCa(CertificateAuthority certificateAuthority, String commonName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        String clusterCaStorePassword = "123456";

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();
        Path clusterCaStoreFile = Files.createTempFile("tls", "cluster-ca-store");
        clusterCaStoreFile.toFile().deleteOnExit();
        
        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(commonName).build();

        CERT_MANAGER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, ModelUtils.getCertificateValidity(certificateAuthority));

        CERT_MANAGER.addCertToTrustStore(clusterCaCertFile.toFile(), CA_CRT, clusterCaStoreFile.toFile(), clusterCaStorePassword);
        return new CertAndKey(
                Files.readAllBytes(clusterCaKeyFile),
                Files.readAllBytes(clusterCaCertFile),
                Files.readAllBytes(clusterCaStoreFile),
                null,
                clusterCaStorePassword);
    }

    private List<Secret> initialClusterCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "cluster-ca",
                AbstractModel.clusterCaKeySecretName(NAME),
                AbstractModel.clusterCaCertSecretName(NAME));
    }

    private List<Secret> initialClientsCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "clients-ca",
                KafkaResources.clientsCaKeySecretName(NAME),
                KafkaResources.clientsCaCertificateSecretName(NAME));
    }

    private List<Secret> initialCaSecrets(CertificateAuthority certificateAuthority, String commonName, String caKeySecretName, String caCertSecretName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey result = generateCa(certificateAuthority, commonName);
        Secret caKeySecret = ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, caKeySecretName, result.keyAsBase64String());
        Secret caCertSecret = ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, caCertSecretName,
                        result.certAsBase64String(), result.trustStoreAsBase64String(), result.storePasswordAsBase64String());

        assertCertDataNotNull(caCertSecret.getData());
        assertThat(isCertInTrustStore(CA_CRT, caCertSecret.getData()), is(true));
        assertKeyDataNotNull(caKeySecret.getData());
        return List.of(caKeySecret, caCertSecret);
    }

    private KeyStore getTrustStore(Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                Util.decodeBytesFromBase64(data.get(CA_STORE))),
                Util.decodeFromBase64(data.get(CA_STORE_PASSWORD)).toCharArray()
        );
        return trustStore;
    }

    private boolean isCertInTrustStore(String alias, Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = getTrustStore(data);
        return trustStore.isCertificateEntry(alias);
    }

    private X509Certificate getCertificateFromTrustStore(String alias, Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = getTrustStore(data);
        return (X509Certificate) trustStore.getCertificate(alias);
    }

    private void assertCaptorSecretsNotNull(CaptorSecrets secrets) {
        assertThat(secrets.clusterCaCert(), is(notNullValue()));
        assertThat(secrets.clusterCaKey(), is(notNullValue()));
        assertThat(secrets.clientsCaCert(), is(notNullValue()));
        assertThat(secrets.clientsCaKey(), is(notNullValue()));
    }

    private CaptorSecrets verifyCaSecretReconcileCalls(SecretOperator secretOps) {
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture());

        return new CaptorSecrets(clusterCaCert.getValue(), clusterCaKey.getValue(), clientsCaCert.getValue(), clientsCaKey.getValue());
    }

    private void assertCertDataNotNull(Map<String, String> certData) {
        assertThat(certData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(certData.get(CA_CRT), is(notNullValue()));
        assertThat(certData.get(CA_STORE), is(notNullValue()));
        assertThat(certData.get(CA_STORE_PASSWORD), is(notNullValue()));
    }

    private void assertKeyDataNotNull(Map<String, String> keyData) {
        assertThat(keyData.keySet(), is(singleton(CA_KEY)));
        assertThat(keyData.get(CA_KEY), is(notNullValue()));
    }

    private record CaptorSecrets(
            Secret clusterCaCert,
            Secret clusterCaKey,
            Secret clientsCaCert,
            Secret clientsCaKey
    ) { }

    //////////
    // Tests Strimzi managed CA
    //////////

    @Test
    public void testReconcileCasGeneratesCertsInitially(Vertx vertx, VertxTestContext context) {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                assertThat(captorSecrets.clusterCaCert().getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(isCertInTrustStore(CA_CRT, captorSecrets.clusterCaCert.getData()), is(true));

                assertThat(captorSecrets.clusterCaKey().getData().keySet(), is(singleton(CA_KEY)));

                assertThat(captorSecrets.clientsCaCert().getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(isCertInTrustStore(CA_CRT, captorSecrets.clientsCaCert().getData()), is(true));

                assertThat(captorSecrets.clientsCaKey().getData().keySet(), is(singleton(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testReconcileCasNoCertsGetGeneratedOutsideRenewalPeriod(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();

        reconcileCas(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                assertThat(captorSecrets.clusterCaCert().getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(captorSecrets.clusterCaCert().getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(x509Certificate(initialClusterCaCertSecret.getData().get(CA_CRT)), is(getCertificateFromTrustStore(CA_CRT, captorSecrets.clusterCaCert().getData())));

                assertThat(captorSecrets.clusterCaKey().getData().keySet(), is(Set.of(CA_KEY)));
                assertThat(captorSecrets.clusterCaKey().getData().get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));

                assertThat(captorSecrets.clientsCaCert().getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(captorSecrets.clientsCaCert().getData().get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(x509Certificate(initialClientsCaCertSecret.getData().get(CA_CRT)), is(getCertificateFromTrustStore(CA_CRT, captorSecrets.clientsCaCert().getData())));

                assertThat(captorSecrets.clientsCaKey().getData().keySet(), is(Set.of(CA_KEY)));
                assertThat(captorSecrets.clientsCaKey().getData().get(CA_KEY), is(initialClientsCaKeySecret.getData().get(CA_KEY)));
                async.flag();
            })));
    }

    @Test
    public void testGenerateTruststoreFromOldSecrets(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        // remove truststore and password to simulate Secrets coming from an older version
        initialClusterCaCertSecret.getData().remove(CA_STORE);
        initialClusterCaCertSecret.getData().remove(CA_STORE_PASSWORD);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        // remove truststore and password to simulate Secrets coming from an older version
        initialClientsCaCertSecret.getData().remove(CA_STORE);
        initialClientsCaCertSecret.getData().remove(CA_STORE_PASSWORD);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                String newClusterCaCert = captorSecrets.clusterCaCert().getData().get(CA_CRT);
                assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(getCertificateFromTrustStore(CA_CRT, captorSecrets.clusterCaCert().getData()), is(x509Certificate(newClusterCaCert)));

                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertThat(clusterCaKeyData, aMapWithSize(1));
                assertThat(clusterCaKeyData, hasEntry(CA_KEY, initialClusterCaKeySecret.getData().get(CA_KEY)));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertCertDataNotNull(clientsCaCertData);

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertThat(clientsCaKeyData, aMapWithSize(1));
                assertThat(clientsCaKeyData, hasEntry(CA_KEY, initialClientsCaKeySecret.getData().get(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();
        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                String newClusterCaCert = captorSecrets.clusterCaCert().getData().get(CA_CRT);
                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(captorSecrets.clusterCaCert().getData().get(CA_STORE_PASSWORD), is(not(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD))));
                assertThat(getCertificateFromTrustStore(CA_CRT, captorSecrets.clusterCaCert().getData()), is(x509Certificate(newClusterCaCert)));

                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertThat(clusterCaKeyData, aMapWithSize(1));
                assertThat(clusterCaKeyData, hasEntry(CA_KEY, initialClusterCaKeySecret.getData().get(CA_KEY)));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertCertDataNotNull(clientsCaCertData);

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(not(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD))));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertThat(clientsCaKeyData, aMapWithSize(1));
                assertThat(clientsCaKeyData, hasEntry(CA_KEY, initialClientsCaKeySecret.getData().get(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAutoOutsideOfMaintenanceWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                String newClusterCaCert = captorSecrets.clusterCaCert().getData().get(CA_CRT);
                assertThat(captorSecrets.clusterCaCert().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
                assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(captorSecrets.clusterCaCert().getData().get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, captorSecrets.clusterCaCert().getData()), is(x509Certificate(newClusterCaCert)));

                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertThat(clusterCaKeyData, aMapWithSize(1));
                assertThat(clusterCaKeyData, hasEntry(CA_KEY, initialClusterCaKeySecret.getData().get(CA_KEY)));
                assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));


                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertCertDataNotNull(clientsCaCertData);

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                assertThat(captorSecrets.clientsCaCert().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
                assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertThat(clientsCaKeyData, aMapWithSize(1));
                assertThat(clientsCaKeyData, hasEntry(CA_KEY, initialClientsCaKeySecret.getData().get(CA_KEY)));
                assertThat(captorSecrets.clientsCaKey().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));

                async.flag();
            })));
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAutoWithinMaintenanceWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T10:12:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                Map<String, String> clusterCaCertData = captorSecrets.clusterCaCert().getData();
                assertCertDataNotNull(clusterCaCertData);

                String newClusterCaCert = clusterCaCertData.get(CA_CRT);
                assertThat(captorSecrets.clusterCaCert().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(not(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD))));
                assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData), is(x509Certificate(newClusterCaCert)));

                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertThat(clusterCaKeyData, aMapWithSize(1));
                assertThat(clusterCaKeyData, hasEntry(CA_KEY, initialClusterCaKeySecret.getData().get(CA_KEY)));
                assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertCertDataNotNull(clientsCaCertData);

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                assertThat(captorSecrets.clientsCaCert().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(not(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD))));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertThat(clientsCaKeyData, aMapWithSize(1));
                assertThat(clientsCaKeyData, hasEntry(CA_KEY, initialClientsCaKeySecret.getData().get(CA_KEY)));
                assertThat(captorSecrets.clientsCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));

                async.flag();
            })));
    }

    @Test
    public void testNewKeyGetGeneratedWhenInRenewalPeriodAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                Map<String, String> clusterCaCertData = captorSecrets.clusterCaCert().getData();
                assertThat(clusterCaCertData, aMapWithSize(4));

                String newClusterCaCert = clusterCaCertData.get(CA_CRT);
                String oldClusterCaCertKey = clusterCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                String oldClusterCaCert = clusterCaCertData.get(oldClusterCaCertKey);

                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData), is(x509Certificate(newClusterCaCert)));

                assertThat(oldClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(getCertificateFromTrustStore(oldClusterCaCertKey, clusterCaCertData), is(x509Certificate(oldClusterCaCert)));
                assertThat(x509Certificate(newClusterCaCert).getSubjectX500Principal().getName(), is("CN=cluster-ca v1,O=io.strimzi"));

                assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertKeyDataNotNull(clusterCaKeyData);
                assertThat(clusterCaKeyData.get(CA_KEY), is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertThat(clientsCaCertData, aMapWithSize(4));

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                String oldClientsCaCertKey = clientsCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                String oldClientsCaCert = clientsCaCertData.get(oldClientsCaCertKey);

                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));

                assertThat(oldClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(getCertificateFromTrustStore(oldClientsCaCertKey, clientsCaCertData), is(x509Certificate(oldClientsCaCert)));
                assertThat(x509Certificate(newClientsCaCert).getSubjectX500Principal().getName(), is("CN=clients-ca v1,O=io.strimzi"));

                assertThat(captorSecrets.clientsCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertKeyDataNotNull(clientsCaKeyData);
                assertThat(clientsCaKeyData.get(CA_KEY), is(not(initialClientsCaKeySecret.getData().get(CA_KEY))));

                async.flag();
            })));
    }

    @Test
    public void testNewKeyGeneratedWhenInRenewalPeriodAutoOutsideOfTimeWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                Map<String, String> clusterCaCertData = captorSecrets.clusterCaCert().getData();
                assertThat(captorSecrets.clusterCaCert().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0"));
                assertThat(clusterCaCertData, aMapWithSize(3));

                String newClusterCaCert = clusterCaCertData.get(CA_CRT);
                assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData), is(x509Certificate(newClusterCaCert)));
                assertThat(x509Certificate(newClusterCaCert).getSubjectX500Principal().getName(), is("CN=cluster-ca,O=io.strimzi"));

                assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));
                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertThat(clusterCaKeyData, aMapWithSize(1));
                assertThat(clusterCaKeyData, hasEntry(CA_KEY, initialClusterCaKeySecret.getData().get(CA_KEY)));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertThat(captorSecrets.clientsCaCert().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0"));
                assertThat(clientsCaCertData, aMapWithSize(3));

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));
                assertThat(x509Certificate(newClientsCaCert).getSubjectX500Principal().getName(), is("CN=clients-ca,O=io.strimzi"));

                assertThat(captorSecrets.clientsCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));
                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertThat(clientsCaKeyData, aMapWithSize(1));
                assertThat(clientsCaKeyData, hasEntry(CA_KEY, initialClientsCaKeySecret.getData().get(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testNewKeyGeneratedWhenInRenewalPeriodAutoWithinTimeWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T09:12:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                Map<String, String> clusterCaCertData = captorSecrets.clusterCaCert().getData();
                assertThat(captorSecrets.clusterCaCert().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                assertThat(clusterCaCertData, aMapWithSize(4));

                String newClusterCaCert = clusterCaCertData.get(CA_CRT);
                String oldClusterCaCertKey = clusterCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                String oldClusterCaCert = clusterCaCertData.get(oldClusterCaCertKey);

                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData), is(x509Certificate(newClusterCaCert)));

                assertThat(oldClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(getCertificateFromTrustStore(oldClusterCaCertKey, clusterCaCertData), is(x509Certificate(oldClusterCaCert)));
                assertThat(x509Certificate(newClusterCaCert).getSubjectX500Principal().getName(), is("CN=cluster-ca v1,O=io.strimzi"));

                assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clusterCaKeyData = captorSecrets.clusterCaKey().getData();
                assertKeyDataNotNull(clusterCaKeyData);
                assertThat(clusterCaKeyData.get(CA_KEY), is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertThat(captorSecrets.clientsCaCert().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                assertThat(clientsCaCertData, aMapWithSize(4));

                String newClientsCaCert = clientsCaCertData.get(CA_CRT);
                String oldClientsCaCertKey = clientsCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                String oldClientsCaCert = clientsCaCertData.get(oldClientsCaCertKey);

                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(newClientsCaCert)));

                assertThat(oldClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(getCertificateFromTrustStore(oldClientsCaCertKey, clientsCaCertData), is(x509Certificate(oldClientsCaCert)));
                assertThat(x509Certificate(newClientsCaCert).getSubjectX500Principal().getName(), is("CN=clients-ca v1,O=io.strimzi"));

                assertThat(captorSecrets.clientsCaKey().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clientsCaKeyData = captorSecrets.clientsCaKey().getData();
                assertKeyDataNotNull(clientsCaKeyData);
                assertThat(clientsCaKeyData.get(CA_KEY), is(not(initialClientsCaKeySecret.getData().get(CA_KEY))));

                async.flag();
            })));
    }

    private X509Certificate x509Certificate(String newClusterCaCert) throws CertificateException {
        return (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(Util.decodeBytesFromBase64(newClusterCaCert)));
    }

    @Test
    public void testExpiredCertsGetRemovedAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        // add an expired certificate to the secret ...
        String clusterCert = Objects.requireNonNull(ReadWriteUtils.readFileFromResources(getClass(), "cluster-ca.crt"));
        String encodedClusterCert = Base64.getEncoder().encodeToString(clusterCert.getBytes(StandardCharsets.UTF_8));
        initialClusterCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt", encodedClusterCert);

        // ... and to the related truststore
        Path certFile = Files.createTempFile("tls", "-cert");
        certFile.toFile().deleteOnExit();
        Path trustStoreFile = Files.createTempFile("tls", "-truststore");
        trustStoreFile.toFile().deleteOnExit();
        Files.write(certFile, Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        Files.write(trustStoreFile, Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE)));
        String trustStorePassword = Util.decodeFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        CERT_MANAGER.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
        initialClusterCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClusterCaCertSecret.getData()), is(true));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        // add an expired certificate to the secret ...
        String clientCert = Objects.requireNonNull(ReadWriteUtils.readFileFromResources(getClass(), "clients-ca.crt"));
        String encodedClientCert = Base64.getEncoder().encodeToString(clientCert.getBytes(StandardCharsets.UTF_8));
        initialClientsCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt", encodedClientCert);

        // ... and to the related truststore
        certFile = Files.createTempFile("tls", "-cert");
        certFile.toFile().deleteOnExit();
        Files.write(certFile, Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        trustStoreFile = Files.createTempFile("tls", "-truststore");
        trustStoreFile.toFile().deleteOnExit();
        Files.write(trustStoreFile, Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE)));
        trustStorePassword = Util.decodeFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        CERT_MANAGER.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
        initialClientsCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClientsCaCertSecret.getData()), is(true));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                assertCaptorSecretsNotNull(captorSecrets);

                Map<String, String> clusterCaCertData = captorSecrets.clusterCaCert().getData();
                assertThat(clusterCaCertData, aMapWithSize(3));
                assertThat(clusterCaCertData.get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData), is(x509Certificate(clusterCaCertData.get(CA_CRT))));
                assertThat(captorSecrets.clusterCaKey().getData().get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));
                assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clusterCaCertData), is(false));

                Map<String, String> clientsCaCertData = captorSecrets.clientsCaCert().getData();
                assertThat(clientsCaCertData, aMapWithSize(3));
                assertThat(clientsCaCertData.get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(clientsCaCertData.get(CA_CRT))));
                assertThat(captorSecrets.clientsCaKey().getData().get(CA_KEY), is(initialClientsCaKeySecret.getData().get(CA_KEY)));
                assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clientsCaCertData), is(false));
                async.flag();
            })));
    }

    @Test
    public void testCustomLabelsAndAnnotations(Vertx vertx, VertxTestContext context) {
        Map<String, String> labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        Map<String, String> annos = new HashMap<>(2);
        annos.put("anno1", "value3");
        annos.put("anno2", "value4");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewClusterCaCert()
                                .withNewMetadata()
                                    .withAnnotations(annos)
                                    .withLabels(labels)
                                .endMetadata()
                            .endClusterCaCert()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, kafka, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                    assertCaptorSecretsNotNull(captorSecrets);

                    Secret clusterCaCertSecret = captorSecrets.clusterCaCert();
                    Secret clusterCaKeySecret = captorSecrets.clusterCaKey();
                    Secret clientsCaCertSecret = captorSecrets.clientsCaCert();
                    Secret clientsCaKeySecret = captorSecrets.clientsCaKey();

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
    public void testClusterCASecretsWithoutOwnerReference(Vertx vertx, VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(caConfig)
                .endSpec()
                .build();

        Checkpoint async = context.checkpoint();

        reconcileCas(vertx, kafka, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);
                    assertCaptorSecretsNotNull(captorSecrets);

                    Secret clusterCaCertSecret = captorSecrets.clusterCaCert();
                    Secret clusterCaKeySecret = captorSecrets.clusterCaKey();
                    Secret clientsCaCertSecret = captorSecrets.clientsCaCert();
                    Secret clientsCaKeySecret = captorSecrets.clientsCaKey();

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences(), hasSize(1));

                    TestUtils.checkOwnerReference(clientsCaCertSecret, kafka);
                    TestUtils.checkOwnerReference(clientsCaKeySecret, kafka);

                    async.flag();
                })));
    }

    @Test
    public void testClientsCASecretsWithoutOwnerReference(Vertx vertx, VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClientsCa(caConfig)
                .endSpec()
                .build();

        Checkpoint async = context.checkpoint();

        reconcileCas(vertx, kafka, Clock.systemUTC())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    CaptorSecrets captorSecrets = verifyCaSecretReconcileCalls(supplier.secretOperations);

                    assertThat(captorSecrets.clusterCaCert().getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(captorSecrets.clusterCaKey().getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(captorSecrets.clientsCaCert().getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(captorSecrets.clientsCaKey().getMetadata().getOwnerReferences(), hasSize(0));

                    TestUtils.checkOwnerReference(captorSecrets.clusterCaCert(), kafka);
                    TestUtils.checkOwnerReference(captorSecrets.clusterCaKey(), kafka);

                    async.flag();
                })));
    }

    //////////
    // Tests user managed CA
    //////////

    @Test
    public void testReconcileCasWhenUserManagedCertsAreMissingThrows(Vertx vertx, VertxTestContext context) {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e, instanceOf(InvalidResourceException.class));
                    assertThat(e.getMessage(), is("Cluster CA should not be generated, but the secrets were not found."));
                    async.flag();
                })));
    }

    @Test
    public void testUserManagedCertsNotReconciled(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(false)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCas(vertx, certificateAuthority, certificateAuthority)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), any(Secret.class));
                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));
                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), any(Secret.class));
                    verify(supplier.secretOperations, times(0)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));
                    async.flag();
                })));
    }
}
