/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CertUtilsTest {
    @Test
    public void testExistingCertificatesDiffer()   {
        Secret defaultSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret sameAsDefaultSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret scaleDownSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .build();

        Secret scaleUpSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .addToData("my-cluster-kafka-3.crt", "Certificate3")
                .addToData("my-cluster-kafka-3.key", "Key3")
                .addToData("my-cluster-kafka-4.crt", "Certificate4")
                .addToData("my-cluster-kafka-4.key", "Key4")
                .build();

        Secret changedSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "NewKey1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret changedScaleUpSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "NewCertificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .addToData("my-cluster-kafka-3.crt", "Certificate3")
                .addToData("my-cluster-kafka-3.key", "Key3")
                .addToData("my-cluster-kafka-4.crt", "Certificate4")
                .addToData("my-cluster-kafka-4.key", "Key4")
                .build();

        Secret changedScaleDownSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "NewCertificate0")
                .addToData("my-cluster-kafka-0.key", "NewKey0")
                .build();

        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, defaultSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, sameAsDefaultSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, scaleDownSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, scaleUpSecret), is(false));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, changedSecret), is(true));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, changedScaleUpSecret), is(true));
        assertThat(CertUtils.doExistingCertificatesDiffer(defaultSecret, changedScaleDownSecret), is(true));
    }

    @Test
    public void testTrustedCertificatesVolumes()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<Volume> volumes = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumes(volumes, List.of(cert1, cert2, cert3), false);

        assertThat(volumes.size(), is(2));
        assertThat(volumes.get(0).getName(), is("first-certificate"));
        assertThat(volumes.get(0).getSecret().getSecretName(), is("first-certificate"));
        assertThat(volumes.get(1).getName(), is("second-certificate"));
        assertThat(volumes.get(1).getSecret().getSecretName(), is("second-certificate"));
    }

    @Test
    public void testShortenedTrustedCertificatesVolumesWithPrefix() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("long-secret-name-60-chars-ooooooooooooooooooooooooooooooooo")
                .withCertificate("first.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("long-secret-name-60-chars-ooooooooooooooooooooooooooooooooo")
                .withCertificate("second.crt")
                .build();

        List<Volume> volumes = new ArrayList<>();

        // if len(prefix + secretname) > 63, then the volume name is shortened.
        // Volume name references in both the volume and volumeMount sections should not be duplicated.
        CertUtils.createTrustedCertificatesVolumes(volumes, List.of(cert1, cert2), false, "some-long-prefix");
        assertThat(volumes.size(), is(1));
    }

    @Test
    public void testTrustedCertificatesVolumesWithPrefix()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<Volume> volumes = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumes(volumes, List.of(cert1, cert2, cert3), false, "prefixed");

        assertThat(volumes.size(), is(2));
        assertThat(volumes.get(0).getName(), is("prefixed-first-certificate"));
        assertThat(volumes.get(0).getSecret().getSecretName(), is("first-certificate"));
        assertThat(volumes.get(1).getName(), is("prefixed-second-certificate"));
        assertThat(volumes.get(1).getSecret().getSecretName(), is("second-certificate"));
    }

    @Test
    public void testShortenedTrustedCertificatesVolumeMountsWithPrefix() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("long-secret-name-60-chars-ooooooooooooooooooooooooooooooooo")
                .withCertificate("first.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("long-secret-name-60-chars-ooooooooooooooooooooooooooooooooo")
                .withCertificate("second.crt")
                .build();

        List<VolumeMount> mounts = new ArrayList<>();

        // if len(prefix + secretname) > 63, then the volume name is shortened.
        // Volume name references in both the volume and volumeMount sections should not be duplicated.
        CertUtils.createTrustedCertificatesVolumeMounts(mounts, List.of(cert1, cert2), "/my/path/", "some-long-prefix");
        assertThat(mounts.size(), is(1));
    }

    @Test
    public void testTrustedCertificatesVolumeMountsWithPrefix()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<VolumeMount> mounts = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumeMounts(mounts, List.of(cert1, cert2, cert3), "/my/path/", "prefixed");

        assertThat(mounts.size(), is(2));
        assertThat(mounts.get(0).getName(), is("prefixed-first-certificate"));
        assertThat(mounts.get(0).getMountPath(), is("/my/path/first-certificate"));
        assertThat(mounts.get(1).getName(), is("prefixed-second-certificate"));
        assertThat(mounts.get(1).getMountPath(), is("/my/path/second-certificate"));
    }

    @Test
    public void testTrustedCertificatesVolumeMounts()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        List<VolumeMount> mounts = new ArrayList<>();
        CertUtils.createTrustedCertificatesVolumeMounts(mounts, List.of(cert1, cert2, cert3), "/my/path/");

        assertThat(mounts.size(), is(2));
        assertThat(mounts.get(0).getName(), is("first-certificate"));
        assertThat(mounts.get(0).getMountPath(), is("/my/path/first-certificate"));
        assertThat(mounts.get(1).getName(), is("second-certificate"));
        assertThat(mounts.get(1).getMountPath(), is("/my/path/second-certificate"));
    }

    @Test
    public void testTrustedCertsEnvVar()    {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        CertSecretSource cert4 = new CertSecretSourceBuilder()
                .withSecretName("third-certificate")
                .withCertificate("*.crt")
                .build();

        CertSecretSource cert5 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("*.pem")
                .build();

        assertThat(CertUtils.trustedCertsEnvVar(List.of(cert1, cert2, cert3, cert4, cert5)), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt;third-certificate/*.crt;first-certificate/*.pem"));
    }

    @Test
    public void testCertIsTrust() throws IOException, CertificateException {
        OpenSslCertManager ssl = new OpenSslCertManager();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = Files.createTempFile("key-", ".key").toFile();
        File rootCert = Files.createTempFile("crt-", ".crt").toFile();
        File alternateRootKey = Files.createTempFile("key-", ".key").toFile();
        File alternateRootCert = Files.createTempFile("crt-", ".crt").toFile();
        File key = Files.createTempFile("key-", ".key").toFile();
        File csr = Files.createTempFile("csr-", ".csr").toFile();
        File cert = Files.createTempFile("crt-", ".crt").toFile();

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        try {

            // Generate a root cert
            Instant now = Instant.now();
            ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
            ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
            ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

            // Generate alternate root cert
            ssl.generateRootCaCert(rootSubject, alternateRootKey, alternateRootCert, notBefore, notAfter, 1);

            Subject subject = new Subject.Builder()
                    .withCommonName("MyCommonName")
                    .withOrganizationName("MyOrganization")
                    .addDnsName("example1.com")
                    .addDnsName("example2.com").build();

            // Generate cert
            ssl.generateCsr(key, csr, subject);
            ssl.generateCert(csr, rootKey, rootCert, cert, subject, 1);

            X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
            X509Certificate x509AlternateRootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(alternateRootCert));
            X509Certificate x509Cert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(cert));

            assertTrue(CertUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, x509Cert, x509RootCert));
            assertFalse(CertUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, x509Cert, x509AlternateRootCert));

        } finally {
            rootKey.delete();
            rootCert.delete();
            alternateRootKey.delete();
            alternateRootCert.delete();
            key.delete();
            csr.delete();
            cert.delete();
        }
    }

    @Test
    public void testCertChainWithIntermediateIsTrusted() throws IOException, CertificateException {
        OpenSslCertManager ssl = new OpenSslCertManager();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = Files.createTempFile("key-", ".key").toFile();
        File rootCert = Files.createTempFile("crt-", ".crt").toFile();
        File intermediateKey = Files.createTempFile("key-", ".key").toFile();
        File intermediateCert = Files.createTempFile("crt-", ".crt").toFile();
        File leafKey = Files.createTempFile("key-", ".key").toFile();
        File csr = Files.createTempFile("csr-", ".csr").toFile();
        File leafCert = Files.createTempFile("crt-", ".crt").toFile();
        File combinedPem = Files.createTempFile("combined-", ".pem").toFile();

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        try {

            // Generate a root cert
            Instant now = Instant.now();
            ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
            ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
            ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

            // Generate an intermediate cert
            Subject intermediateSubject = new Subject.Builder().withCommonName("IntermediateCn").withOrganizationName("MyOrganization").build();
            ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject, intermediateKey, intermediateCert, notBefore, notAfter, 1);

            Subject subject = new Subject.Builder()
                    .withCommonName("MyCommonName")
                    .withOrganizationName("MyOrganization")
                    .addDnsName("example1.com")
                    .addDnsName("example2.com").build();

            // Generate leaf cert
            ssl.generateCsr(leafKey, csr, subject);
            ssl.generateCert(csr, intermediateKey, intermediateCert, leafCert, subject, 1);

            X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
            X509Certificate x509LeafCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(leafCert));

            assertFalse(CertUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, x509LeafCert, x509RootCert));

            // Construct combined pem to validate
            try (FileInputStream intCertFis = new FileInputStream(intermediateCert);
                 FileInputStream leafCertFis = new FileInputStream(leafCert);
                 FileOutputStream combinedFos = new FileOutputStream(combinedPem)) {
                String combined = String.join("\n",
                        new String(intCertFis.readAllBytes(), StandardCharsets.US_ASCII),
                        new String(leafCertFis.readAllBytes(), StandardCharsets.US_ASCII));
                combinedFos.write(combined.getBytes(StandardCharsets.US_ASCII));
                X509Certificate combinedCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(combinedPem));

                assertTrue(CertUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, combinedCert, x509RootCert));
            }

        } finally {
            rootKey.delete();
            rootCert.delete();
            intermediateKey.delete();
            intermediateCert.delete();
            leafKey.delete();
            csr.delete();
            leafCert.delete();
            combinedPem.delete();
        }
    }
}
