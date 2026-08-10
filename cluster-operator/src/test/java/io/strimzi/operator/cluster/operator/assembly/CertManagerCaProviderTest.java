/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateManagerType;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.StrimziSubject;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.ca.CertManagerCa;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertIssuer;
import io.strimzi.operator.common.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;

import static io.strimzi.operator.common.ca.Ca.CA_CRT;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CertManagerCaProviderTest {
    private static final String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    private static final String NAME = Reconciliation.DUMMY_RECONCILIATION.name();
    private static final String CM_CA_CERT_SECRET_NAME = "cert-manager-ca-cert";
    private static final CertificateAuthority CERT_AUTHORITY = new CertificateAuthorityBuilder()
            .withValidityDays(100)
            .withRenewalDays(10)
            .withGenerateCertificateAuthority(false)
            .withType(CertificateManagerType.CERT_MANAGER_IO)
            .withNewCertManager()
                .withNewCaCert()
                    .withSecretName(CM_CA_CERT_SECRET_NAME)
                    .withCertificate(CA_CRT)
                .endCaCert()
            .endCertManager()
            .build();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withClusterCa(CERT_AUTHORITY)
                .withClientsCa(CERT_AUTHORITY)
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
    private static final OpenSslCertIssuer CERT_ISSUER = new OpenSslCertIssuer();

    private SecretOperator secretOperations;
    private CertManagerCertificateOperator certificateOperator;

    @BeforeEach
    public void setup() {
        secretOperations = mock(SecretOperator.class);
        certificateOperator = mock(CertManagerCertificateOperator.class);
    }

    private CertAndKey generateCa(String commonName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        String clusterCaStorePassword = "123456";

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();
        Path clusterCaStoreFile = Files.createTempFile("tls", "cluster-ca-store");
        clusterCaStoreFile.toFile().deleteOnExit();

        StrimziSubject sbj = new StrimziSubject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(commonName).build();

        CERT_ISSUER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, CERT_AUTHORITY.getValidityDays());

        CERT_ISSUER.addCertToTrustStore(clusterCaCertFile.toFile(), CA_CRT, clusterCaStoreFile.toFile(), clusterCaStorePassword);
        return new CertAndKey(
                Files.readAllBytes(clusterCaKeyFile),
                Files.readAllBytes(clusterCaCertFile),
                Files.readAllBytes(clusterCaStoreFile),
                null,
                clusterCaStorePassword);
    }

    private CertAndKey renewCaCert(CertAndKey certAndKey) throws IOException {
        Path caKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        caKeyFile.toFile().deleteOnExit();
        Files.write(caKeyFile, certAndKey.key());
        Path caCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        caCertFile.toFile().deleteOnExit();
        Files.write(caCertFile, certAndKey.cert());

        StrimziSubject sbj = new StrimziSubject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-ca").build();

        CERT_ISSUER.renewSelfSignedCert(caKeyFile.toFile(), caCertFile.toFile(), sbj, 10);

        return new CertAndKey(
                Files.readAllBytes(caKeyFile),
                Files.readAllBytes(caCertFile),
                null,
                null,
                null);
    }

    private CertAndKey generateClusterOperatorCert(CertAndKey ca) throws IOException {
        File csrFile = Files.createTempFile("tls", "csr").toFile();
        csrFile.deleteOnExit();
        File keyFile = Files.createTempFile("tls", "key").toFile();
        keyFile.deleteOnExit();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        certFile.deleteOnExit();

        StrimziSubject sbj = new StrimziSubject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("cluster-operator").build();

        CERT_ISSUER.generateCsr(keyFile, csrFile, sbj);
        CERT_ISSUER.generateCert(csrFile, ca.key(), ca.cert(), certFile, sbj, 10);

        return new CertAndKey(
                Files.readAllBytes(keyFile.toPath()),
                Files.readAllBytes(certFile.toPath()),
                null,
                null,
                null);
    }

    private static Secret createInitialClusterCaCertSecret(String caCert) throws CertificateException {
        String hash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeFromBase64(caCert).getBytes(StandardCharsets.UTF_8)));
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractModel.clusterCaCertSecretName(NAME))
                    .withNamespace(NAMESPACE)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, hash)
                .endMetadata()
                .addToData("ca.crt", caCert)
                .build();
    }

    private static Secret createInitialClientsCaCertSecret(String caCert) throws CertificateException {
        String hash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeFromBase64(caCert).getBytes(StandardCharsets.UTF_8)));
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clientsCaCertificateSecretName(NAME))
                    .withNamespace(NAMESPACE)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, hash)
                .endMetadata()
                .addToData("ca.crt", caCert)
                .build();
    }

    @ParameterizedTest
    @EnumSource(Ca.CaRole.class)
    public void throwsWhenCertManagerPropertyMissing(Ca.CaRole caRole) {
        CertificateAuthority ca = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .build();

        Kafka kafkaCluster = switch (caRole) {
            case CLUSTER_CA -> new KafkaBuilder(KAFKA).editSpec().withClusterCa(ca).endSpec().build();
            case CLIENTS_CA -> new KafkaBuilder(KAFKA).editSpec().withClientsCa(ca).endSpec().build();
        };

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                caRole,
                new CaConfig(CERT_AUTHORITY, false),
                kafkaCluster,
                null,
                null,
                certificateOperator,
                secretOperations
        );

        Exception exception = assertThrows(CompletionException.class, () -> caProvider.createAndReconcileCa().toCompletableFuture().join());
        assertThat(exception.getCause(), instanceOf(InvalidResourceException.class));
        assertThat(exception.getCause().getMessage(), is("When CA type is set to cert-manager.io, certManager property is required (e.g. clusterCa.certManager)."));
    }

    @ParameterizedTest
    @EnumSource(Ca.CaRole.class)
    public void throwsWhenCaCertSecretMissing(Ca.CaRole caRole) {
        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                caRole,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                null,
                null,
                certificateOperator,
                secretOperations
        );

        Exception exception = assertThrows(CompletionException.class, () -> caProvider.createAndReconcileCa().toCompletableFuture().join());
        assertThat(exception.getCause(), instanceOf(InvalidResourceException.class));
        assertThat(exception.getCause().getMessage(), is("CA public certificate Secret " + CM_CA_CERT_SECRET_NAME + " missing."));
    }

    @ParameterizedTest
    @EnumSource(Ca.CaRole.class)
    public void throwsWhenCaCertSecretDataMissing(Ca.CaRole caRole) {
        String caCertSecretKey = "cm-ca.crt";
        CertificateAuthority ca = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .withType(CertificateManagerType.CERT_MANAGER_IO)
                .withNewCertManager()
                    .withNewCaCert()
                        .withSecretName(CM_CA_CERT_SECRET_NAME)
                        .withCertificate(caCertSecretKey)
                    .endCaCert()
                .endCertManager()
                .build();

        Secret caCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, Map.of(), Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(caCertSecret));

        Kafka kafkaCluster = switch (caRole) {
            case CLUSTER_CA -> new KafkaBuilder(KAFKA).editSpec().withClusterCa(ca).endSpec().build();
            case CLIENTS_CA -> new KafkaBuilder(KAFKA).editSpec().withClientsCa(ca).endSpec().build();
        };

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                caRole,
                new CaConfig(ca, false),
                kafkaCluster,
                null,
                null,
                certificateOperator,
                secretOperations
        );

        Exception exception = assertThrows(CompletionException.class, () -> caProvider.createAndReconcileCa().toCompletableFuture().join());
        assertThat(exception.getCause(), instanceOf(InvalidResourceException.class));
        assertThat(exception.getCause().getMessage(), is("CA public certificate Secret " + CM_CA_CERT_SECRET_NAME + " missing key " + caCertSecretKey));
    }

    @Test
    public void createsClusterCaSecretInitially() throws CertificateException {
        Map<String, String> caCertData = Map.of(CA_CRT, MockCertIssuer.clusterCaCert());

        Secret userCaCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, caCertData, Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecret));

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                null,
                null,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(caCertData));
        assertThat(result.ca().caCertGeneration(), is(0));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(caCertData));
        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(MockCertIssuer.clusterCaCert())));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void createsClientsCaSecretInitially() throws CertificateException {
        Map<String, String> caCertData = Map.of(CA_CRT, MockCertIssuer.clientsCaCert());

        Secret userCaCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, caCertData, Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecret));

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLIENTS_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                null,
                null,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(caCertData));
        assertThat(result.ca().caCertGeneration(), is(0));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(caCertData));
        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        // Clients Ca cert secret does not need key annotation
        assertThat(Annotations.hasAnnotation(result.certSecret(), Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is(false));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(MockCertIssuer.clientsCaCert())));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void noChangeToClusterCaSecret() throws CertificateException {
        Map<String, String> caCertData = Map.of(CA_CRT, MockCertIssuer.clusterCaCert());

        Secret userCaCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, caCertData, Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecret));

        Secret existingCaCertSecret = createInitialClusterCaCertSecret(MockCertIssuer.clusterCaCert());
        Secret clusterOperatorSecret = ModelUtils.createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
                NAMESPACE,
                Labels.EMPTY,
                null,
                Map.of("cluster-operator.crt", Util.encodeToBase64(MockCertIssuer.serverCert()),
                        "cluster-operator.key", Util.encodeToBase64(MockCertIssuer.serverKey())),
                Map.of(),
                Map.of());

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                clusterOperatorSecret,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(caCertData));
        assertThat(result.ca().caCertGeneration(), is(0));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(caCertData));
        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(MockCertIssuer.clusterCaCert())));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void noChangeToClientsCaSecret() throws CertificateException {
        Map<String, String> caCertData = Map.of(CA_CRT, MockCertIssuer.clientsCaCert());

        Secret userCaCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, caCertData, Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecret));

        Secret existingCaCertSecret = createInitialClientsCaCertSecret(MockCertIssuer.clientsCaCert());
        Secret clusterOperatorSecret = ModelUtils.createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
                NAMESPACE,
                Labels.EMPTY,
                null,
                Map.of("cluster-operator.crt", Util.encodeToBase64(MockCertIssuer.serverCert()),
                        "cluster-operator.key", Util.encodeToBase64(MockCertIssuer.serverKey())),
                Map.of(),
                Map.of());

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLIENTS_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                clusterOperatorSecret,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(caCertData));
        assertThat(result.ca().caCertGeneration(), is(0));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(caCertData));
        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        // Clients Ca cert secret does not need key annotation
        assertThat(Annotations.hasAnnotation(result.certSecret(), Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is(false));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(MockCertIssuer.clientsCaCert())));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void clusterCaSecretRenewed() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey initialCaCert = generateCa(Ca.CaRole.CLUSTER_CA.caCommonName());
        CertAndKey renewedCaCert = renewCaCert(initialCaCert);
        Map<String, String> renewedCaCertData = Map.of(CA_CRT, renewedCaCert.certAsBase64String());

        Secret userCaCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, renewedCaCertData, Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecret));

        Secret existingCaCertSecret = createInitialClusterCaCertSecret(initialCaCert.certAsBase64String());
        CertAndKey clusterOperatorCert = generateClusterOperatorCert(initialCaCert);
        Secret clusterOperatorSecret = ModelUtils.createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
                NAMESPACE,
                Labels.EMPTY,
                null,
                Map.of("cluster-operator.crt", clusterOperatorCert.certAsBase64String(),
                        "cluster-operator.key", clusterOperatorCert.keyAsBase64String()),
                Map.of(),
                Map.of());

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                clusterOperatorSecret,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(renewedCaCertData));
        assertThat(result.ca().caCertGeneration(), is(1));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(renewedCaCertData));
        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(renewedCaCertData.get(CA_CRT))));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void clientsCaSecretRenewed() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey initialCa = generateCa(Ca.CaRole.CLIENTS_CA.caCommonName());
        CertAndKey renewedCaCert = renewCaCert(initialCa);
        Map<String, String> renewedCaCertData = Map.of(CA_CRT, renewedCaCert.certAsBase64String());

        Secret userCaCertSecret = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE,  Labels.EMPTY, null, renewedCaCertData, Map.of(), Map.of());
        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecret));

        Secret existingCaCertSecret = createInitialClientsCaCertSecret(initialCa.certAsBase64String());

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLIENTS_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                null,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(renewedCaCertData));
        assertThat(result.ca().caCertGeneration(), is(1));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(renewedCaCertData));
        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        // Clients Ca cert secret does not need key annotation
        assertThat(Annotations.hasAnnotation(result.certSecret(), Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is(false));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(renewedCaCertData.get(CA_CRT))));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }


    @Test
    public void clusterCaSecretWithNewKeyAndCert() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey initialClusterCa = generateCa(Ca.CaRole.CLUSTER_CA.caCommonName());
        CertAndKey renewedClusterCa = generateCa(Ca.CaRole.CLUSTER_CA.caCommonName());
        Map<String, String> renewedCaCertData = Map.of(CA_CRT, renewedClusterCa.certAsBase64String());

        Secret existingCaCertSecret = createInitialClusterCaCertSecret(initialClusterCa.certAsBase64String());
        Secret userCaCertSecretRenewed = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE, Labels.EMPTY, null, renewedCaCertData, Map.of(), Map.of());

        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecretRenewed));

        CertAndKey clusterOperatorCert = generateClusterOperatorCert(initialClusterCa);
        Secret clusterOperatorSecret = ModelUtils.createSecret(KafkaResources.clusterOperatorCertsSecretName(NAME),
                NAMESPACE,
                Labels.EMPTY,
                null,
                Map.of("cluster-operator.crt", clusterOperatorCert.certAsBase64String(),
                        "cluster-operator.key", clusterOperatorCert.keyAsBase64String()),
                Map.of(),
                Map.of());

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                clusterOperatorSecret,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData().get(CA_CRT), is(renewedClusterCa.certAsBase64String()));
        assertThat(result.ca().caCertGeneration(), is(1));
        assertThat(result.ca().caKeyGeneration(), is(1));

        assertThat(result.certSecret(), notNullValue());
        Map<String, String> reconciledCaCert = result.certSecret().getData();
        assertThat(reconciledCaCert.size(), is(2));
        assertThat(reconciledCaCert.get(CA_CRT), is(renewedClusterCa.certAsBase64String()));
        reconciledCaCert.remove(CA_CRT);
        Pattern oldCaCertKeyPattern = Pattern.compile("ca-[0-9]+-[0-9]+-[0-9]+T[0-9]+-[0-9]+-[0-9]+Z\\.crt");
        String oldCaCertKey = reconciledCaCert.keySet().stream().findFirst().orElse("");
        assertThat(oldCaCertKeyPattern.matcher(oldCaCertKey).matches(), is(true));
        assertThat(reconciledCaCert.get(oldCaCertKey), is(existingCaCertSecret.getData().get(CA_CRT)));

        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(renewedCaCertData.get(CA_CRT))));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void clientsCaSecretWithNewKeyAndCert() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey initialClientsCa = generateCa(Ca.CaRole.CLIENTS_CA.caCommonName());
        CertAndKey renewedClientsCa = generateCa(Ca.CaRole.CLIENTS_CA.caCommonName());
        Map<String, String> renewedCaCertData = Map.of(CA_CRT, renewedClientsCa.certAsBase64String());

        Secret existingCaCertSecret = createInitialClientsCaCertSecret(initialClientsCa.certAsBase64String());
        Secret userCaCertSecretRenewed = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE, Labels.EMPTY, null, renewedCaCertData, Map.of(), Map.of());

        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecretRenewed));

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLIENTS_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                null,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(renewedCaCertData));
        assertThat(result.ca().caCertGeneration(), is(1));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret(), notNullValue());
        assertThat(result.certSecret().getData(), is(renewedCaCertData));

        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), nullValue());
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(renewedCaCertData.get(CA_CRT))));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void clusterCaSecretNotRenewedWhenCLusterOperatorCertMissing() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey initialClusterCa = generateCa(Ca.CaRole.CLUSTER_CA.caCommonName());
        CertAndKey renewedClusterCa = renewCaCert(initialClusterCa);
        Map<String, String> renewedCaCertData = Map.of(CA_CRT, renewedClusterCa.certAsBase64String());

        Secret existingCaCertSecret = createInitialClusterCaCertSecret(initialClusterCa.certAsBase64String());
        Secret userCaCertSecretRenewed = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE, Labels.EMPTY, null, renewedCaCertData, Map.of(), Map.of());

        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecretRenewed));

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                null,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(existingCaCertSecret.getData()));
        assertThat(result.ca().caCertGeneration(), is(0));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret().getData(), is(existingCaCertSecret.getData()));

        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(initialClusterCa.cert()));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }

    @Test
    public void clusterCaSecretWithNewKeyNotRenewedWhenCLusterOperatorCertMissing() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey initialClusterCa = generateCa(Ca.CaRole.CLUSTER_CA.caCommonName());
        CertAndKey renewedClusterCa = generateCa(Ca.CaRole.CLUSTER_CA.caCommonName());
        Map<String, String> renewedCaCertData = Map.of(CA_CRT, renewedClusterCa.certAsBase64String());

        Secret existingCaCertSecret = createInitialClusterCaCertSecret(initialClusterCa.certAsBase64String());
        Secret userCaCertSecretRenewed = ModelUtils.createSecret(CM_CA_CERT_SECRET_NAME, NAMESPACE, Labels.EMPTY, null, renewedCaCertData, Map.of(), Map.of());

        when(secretOperations.getAsync(eq(NAMESPACE), eq(CM_CA_CERT_SECRET_NAME))).thenReturn(CompletableFuture.completedFuture(userCaCertSecretRenewed));

        CertManagerCaProvider caProvider = new CertManagerCaProvider(Reconciliation.DUMMY_RECONCILIATION,
                Ca.CaRole.CLUSTER_CA,
                new CaConfig(CERT_AUTHORITY, false),
                KAFKA,
                existingCaCertSecret,
                null,
                certificateOperator,
                secretOperations
        );

        CaProviderResult result = caProvider.createAndReconcileCa().toCompletableFuture().join();

        // Verify result
        assertThat(result, notNullValue());

        assertThat(result.ca(), instanceOf(CertManagerCa.class));
        assertThat(result.ca().caCertData(), is(existingCaCertSecret.getData()));
        assertThat(result.ca().caCertGeneration(), is(0));
        assertThat(result.ca().caKeyGeneration(), is(0));

        assertThat(result.certSecret().getData(), is(existingCaCertSecret.getData()));

        Map<String, String> secretAnnotations = result.certSecret().getMetadata().getAnnotations();
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat(secretAnnotations.get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String caCertHash = CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(initialClusterCa.cert()));
        assertThat(secretAnnotations.get(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH), is(caCertHash));

        // Verify K8s calls
        ArgumentCaptor<Secret> caCertSecret = ArgumentCaptor.forClass(Secret.class);
        verify(secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), caCertSecret.capture());
        verify(secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), any(Secret.class));

        assertThat(caCertSecret.getValue(), is(result.certSecret()));
    }
}
