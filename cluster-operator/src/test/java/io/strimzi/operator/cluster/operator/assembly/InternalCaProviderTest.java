/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.TestUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import io.strimzi.test.ReadWriteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.strimzi.operator.common.ca.Ca.CA_CRT;
import static io.strimzi.operator.common.ca.Ca.CA_KEY;
import static io.strimzi.operator.common.ca.InternalCa.CA_STORE;
import static io.strimzi.operator.common.ca.InternalCa.CA_STORE_PASSWORD;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InternalCaProviderTest {
    private static final String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    private static final String NAME = Reconciliation.DUMMY_RECONCILIATION.name();
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

    private final static OpenSslCertIssuer CERT_ISSUER = new OpenSslCertIssuer();
    private final static PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    private SecretOperator secretOperations;

    @BeforeEach
    public void setup() {
        secretOperations = mock(SecretOperator.class);
    }

    private void reconcileCas(CertificateAuthority clusterCa, CertificateAuthority clientsCa, CaSecrets caSecrets) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(clusterCa)
                    .withClientsCa(clientsCa)
                .endSpec()
                .build();

        reconcileCas(kafka, Clock.systemUTC(), caSecrets);
    }

    private void reconcileCas(Kafka kafka, Clock clock, CaSecrets caSecrets) {
        reconcileCas(kafka, clock, true, caSecrets);
    }

    private void reconcileCas(Kafka kafka, Clock clock, boolean generatePkcs12Stores, CaSecrets caSecrets) {
        InternalCaProvider clusterCaProvider = new InternalCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(kafka.getSpec().getClusterCa(), generatePkcs12Stores),
                kafka,
                secretOperations,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                clock,
                caSecrets == null ? null : caSecrets.clusterCaCert,
                caSecrets == null ? null : caSecrets.clusterCaKey
        );

        clusterCaProvider.createAndReconcileCa().toCompletableFuture().join();

        InternalCaProvider clientsCaProvider = new InternalCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLIENTS_CA,
                new CaConfig(kafka.getSpec().getClientsCa(), generatePkcs12Stores),
                kafka,
                secretOperations,
                CERT_ISSUER,
                PASSWORD_GENERATOR,
                clock,
                caSecrets == null ? null : caSecrets.clientsCaCert,
                caSecrets == null ? null : caSecrets.clientsCaKey
        );

        clientsCaProvider.createAndReconcileCa().toCompletableFuture().join();
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

        CERT_ISSUER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, certificateAuthority.getValidityDays());

        CERT_ISSUER.addCertToTrustStore(clusterCaCertFile.toFile(), CA_CRT, clusterCaStoreFile.toFile(), clusterCaStorePassword);
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

    private void assertCaptorSecretsNotNull(CaSecrets secrets) {
        assertThat(secrets.clusterCaCert(), is(notNullValue()));
        assertThat(secrets.clusterCaKey(), is(notNullValue()));
        assertThat(secrets.clientsCaCert(), is(notNullValue()));
        assertThat(secrets.clientsCaKey(), is(notNullValue()));
    }

    private CaSecrets verifyCaSecretReconcileCalls(SecretOperator secretOps) {
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());
        verify(secretOps).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture());

        return new CaSecrets(clusterCaCert.getValue(), clusterCaKey.getValue(), clientsCaCert.getValue(), clientsCaKey.getValue());
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

    private record CaSecrets(
            Secret clusterCaCert,
            Secret clusterCaKey,
            Secret clientsCaCert,
            Secret clientsCaKey
    ) { }

    @Test
    public void testReconcileCasGeneratesCertsInitially() throws CertificateException, KeyStoreException, NoSuchAlgorithmException, IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        reconcileCas(certificateAuthority, certificateAuthority, null);

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
        assertCaptorSecretsNotNull(captorSecrets);

        assertThat(captorSecrets.clusterCaCert().getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(isCertInTrustStore(CA_CRT, captorSecrets.clusterCaCert.getData()), is(true));

        assertThat(captorSecrets.clusterCaKey().getData().keySet(), is(singleton(CA_KEY)));

        assertThat(captorSecrets.clientsCaCert().getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(isCertInTrustStore(CA_CRT, captorSecrets.clientsCaCert().getData()), is(true));

        assertThat(captorSecrets.clientsCaKey().getData().keySet(), is(singleton(CA_KEY)));
    }

    @Test
    public void testReconcileCasNoCertsGetGeneratedOutsideRenewalPeriod()
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

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                        initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testGenerateNewCasWithoutPkcs12() {
        reconcileCas(KAFKA, Clock.systemUTC(), false, null);

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
        assertCaptorSecretsNotNull(captorSecrets);

        assertThat(captorSecrets.clusterCaCert().getData().keySet(), is(Set.of("ca.crt")));
        assertThat(captorSecrets.clientsCaCert().getData().keySet(), is(Set.of("ca.crt")));
    }

    @Test
    public void testExistingCasRemovePkcs12()
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        List<Secret> clusterCaSecrets = initialClusterCaSecrets(new CertificateAuthorityBuilder().withValidityDays(365).withRenewalDays(30).build());
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(new CertificateAuthorityBuilder().withValidityDays(365).withRenewalDays(30).build());
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        reconcileCas(KAFKA, Clock.systemUTC(), false, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
        assertCaptorSecretsNotNull(captorSecrets);

        assertThat(captorSecrets.clusterCaCert().getData().keySet(), is(Set.of("ca.crt")));
        assertThat(captorSecrets.clientsCaCert().getData().keySet(), is(Set.of("ca.crt")));
    }

    @Test
    public void testGenerateTruststoreFromOldSecrets()
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

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAuto()
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

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAutoOutsideOfMaintenanceWindow()
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

        reconcileCas(kafka, Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()),
                new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret, initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAutoWithinMaintenanceWindow()
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

        reconcileCas(kafka, Clock.fixed(Instant.parse("2018-11-26T10:12:00Z"), Clock.systemUTC().getZone()),
                new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret, initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testNewKeyGetGeneratedWhenInRenewalPeriodAuto()
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

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testNewKeyGeneratedWhenInRenewalPeriodAutoOutsideOfTimeWindow()
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

        reconcileCas(kafka, Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()),
                new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret, initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testNewKeyGeneratedWhenInRenewalPeriodAutoWithinTimeWindow()
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

        reconcileCas(kafka, Clock.fixed(Instant.parse("2018-11-26T09:12:00Z"), Clock.systemUTC().getZone()),
                new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret, initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    private X509Certificate x509Certificate(String newClusterCaCert) throws CertificateException {
        return (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(Util.decodeBytesFromBase64(newClusterCaCert)));
    }

    @Test
    public void testExpiredCertsGetRemovedAuto()
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
        CERT_ISSUER.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
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
        CERT_ISSUER.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
        initialClientsCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClientsCaCertSecret.getData()), is(true));

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                initialClientsCaCertSecret, initialClientsCaKeySecret));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testCustomLabelsAndAnnotations() {
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

        reconcileCas(kafka, Clock.systemUTC(), null);

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testClusterCASecretsWithoutOwnerReference() {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(caConfig)
                .endSpec()
                .build();

        reconcileCas(kafka, Clock.systemUTC(), null);

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
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
    }

    @Test
    public void testClientsCASecretsWithoutOwnerReference() {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                .withClientsCa(caConfig)
                .endSpec()
                .build();

        reconcileCas(kafka, Clock.systemUTC(), null);

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);

        assertThat(captorSecrets.clusterCaCert().getMetadata().getOwnerReferences(), hasSize(1));
        assertThat(captorSecrets.clusterCaKey().getMetadata().getOwnerReferences(), hasSize(1));
        assertThat(captorSecrets.clientsCaCert().getMetadata().getOwnerReferences(), hasSize(0));
        assertThat(captorSecrets.clientsCaKey().getMetadata().getOwnerReferences(), hasSize(0));

        TestUtils.checkOwnerReference(captorSecrets.clusterCaCert(), kafka);
        TestUtils.checkOwnerReference(captorSecrets.clusterCaKey(), kafka);
    }

    @Test
    public void testForceReplaceAnnotationIncrementsKeyAndCertGeneration()
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                clientsCaSecrets.get(1), clientsCaSecrets.get(0)));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
        assertCaptorSecretsNotNull(captorSecrets);

        assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
        assertThat(captorSecrets.clusterCaCert().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(captorSecrets.clusterCaCert().getData().get(CA_CRT), is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
        assertThat(captorSecrets.clusterCaKey().getData().get(CA_KEY), is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));
    }

    @Test
    public void testForceRenewAnnotationIncrementsCertGeneration()
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

        reconcileCas(certificateAuthority, certificateAuthority, new CaSecrets(initialClusterCaCertSecret, initialClusterCaKeySecret,
                clientsCaSecrets.get(1), clientsCaSecrets.get(0)));

        CaSecrets captorSecrets = verifyCaSecretReconcileCalls(secretOperations);
        assertCaptorSecretsNotNull(captorSecrets);

        assertThat(captorSecrets.clusterCaCert().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(captorSecrets.clusterCaKey().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        assertThat(captorSecrets.clusterCaCert().getData().get(CA_CRT), is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
        assertThat(captorSecrets.clusterCaKey().getData().get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));
    }
}
