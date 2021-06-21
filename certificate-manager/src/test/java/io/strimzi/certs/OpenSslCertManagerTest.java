/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.SignatureException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OpenSslCertManagerTest {

    private static CertificateFactory certFactory;
    private static OpenSslCertManager ssl;

    @BeforeAll
    public static void before() throws CertificateException {
        Assumptions.assumeTrue(System.getProperty("os.name").contains("nux"));
        certFactory = CertificateFactory.getInstance("X.509");
        ssl = new OpenSslCertManager();
    }

    interface Cmd {
        void exec() throws IOException;
    }

    @Test
    public void testGenerateRootCaCertWithDays() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".p12");
        Subject sbj = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();

        ((Cmd) () -> ssl.generateSelfSignedCert(key, cert, sbj, 365)).exec();
        ssl.addCertToTrustStore(cert, "ca", store, "123456");

        X509Certificate x509Certificate1 = loadCertificate(cert);
        assertTrue(selfVerifies(x509Certificate1),
                "Unexpected self-verification");
        assertEquals(x509Certificate1.getSubjectDN(), x509Certificate1.getIssuerDN(), "Unexpected self-signedness");
        assertSubject(sbj, x509Certificate1);
        X509Certificate x509Certificate = x509Certificate1;
        assertEquals(0, x509Certificate.getBasicConstraints(),
                "Expected a certificate with CA:" + true + ", but basic constraints = " + x509Certificate.getBasicConstraints());

        // truststore verification
        KeyStore store1 = KeyStore.getInstance("PKCS12");
        store1.load(new FileInputStream(store), "123456".toCharArray());
        X509Certificate storeCert = (X509Certificate) store1.getCertificate("ca");
        storeCert.verify(storeCert.getPublicKey());

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testGenerateRootCaCertWithDates() throws Exception {
        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".p12");
        Subject sbj = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();

        Instant now = Instant.now();
        ZonedDateTime notBefore = now.plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(OpenSslCertManager.UTC);
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(OpenSslCertManager.UTC);
        ssl.generateRootCaCert(key, cert, sbj, notBefore, notAfter, 0);
        ssl.addCertToTrustStore(cert, "ca", store, "123456");

        // cert verification
        X509Certificate x509Certificate = loadCertificate(cert);
        assertTrue(selfVerifies(x509Certificate),
                "Unexpected self-verification");
        assertEquals(x509Certificate.getSubjectDN(), x509Certificate.getIssuerDN(), "Expected self-signed certificate");
        assertSubject(sbj, x509Certificate);
        assertEquals(0, x509Certificate.getBasicConstraints(),
                "Expected a certificate with CA:" + true + ", but basic constraints = " + x509Certificate.getBasicConstraints());
        assertEquals(notBefore.toInstant(), x509Certificate.getNotBefore().toInstant());
        assertEquals(notAfter.toInstant(), x509Certificate.getNotAfter().toInstant());

        // truststore verification
        KeyStore store1 = KeyStore.getInstance("PKCS12");
        store1.load(new FileInputStream(store), "123456".toCharArray());
        X509Certificate storeCert = (X509Certificate) store1.getCertificate("ca");
        storeCert.verify(storeCert.getPublicKey());

        key.delete();
        cert.delete();
        store.delete();
    }

    private X509Certificate loadCertificate(File cert) throws CertificateException, FileNotFoundException {
        Certificate c1 = certFactory.generateCertificate(new FileInputStream(cert));
        assertTrue(c1 instanceof X509Certificate);
        X509Certificate x509Certificate = (X509Certificate) c1;
        return x509Certificate;
    }

    private void assertCaCertificate(X509Certificate x509Certificate, boolean expectCa) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {
        assertEquals(expectCa, selfVerifies(x509Certificate),
                "Unexpected self-verification");
        assertEquals(expectCa, x509Certificate.getIssuerDN().equals(x509Certificate.getSubjectDN()),
                "Unexpected self-signedness");
        assertEquals(expectCa, x509Certificate.getBasicConstraints() >= 0,
                "Expected a certificate with CA:" + expectCa + ", but basic constraints = " + x509Certificate.getBasicConstraints());
    }

    private boolean selfVerifies(X509Certificate x509Certificate) throws CertificateException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException {
        boolean isSelfSigned;
        try {
            x509Certificate.verify(x509Certificate.getPublicKey());
            isSelfSigned = true;
        } catch (SignatureException | InvalidKeyException e) {
            isSelfSigned = false;
        }
        return isSelfSigned;
    }

    private void assertSubject(Subject sbj, X509Certificate x509Certificate) throws CertificateParsingException {
        Principal p = x509Certificate.getSubjectDN();
        assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));

        assertSubjectAlternativeNames(sbj, x509Certificate);
    }

    private void assertSubjectAlternativeNames(Subject sbj, X509Certificate x509Certificate) throws CertificateParsingException {
        if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
            final Collection<List<?>> sans = x509Certificate.getSubjectAlternativeNames();
            assertThat(sans, is(notNullValue()));
            assertThat(sbj.subjectAltNames().size(), is(sans.size()));
            for (final List<?> sanItem : sans) {
                assertThat(sbj.subjectAltNames().containsValue(sanItem.get(1)), is(true));
            }
        }
    }

    private void assertIssuer(Subject sbj, X509Certificate x509Certificate) {
        Principal p = x509Certificate.getIssuerDN();
        assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));
    }

    @Test
    public void testGenerateIntermediateCaCertWithDates() throws Exception {

        File rootKey = File.createTempFile("key-", ".key");
        File rootCert = File.createTempFile("crt-", ".crt");
        File intermediateKey = File.createTempFile("key-", ".key");
        File intermediateCert = File.createTempFile("crt-", ".crt");
        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(OpenSslCertManager.UTC);
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(OpenSslCertManager.UTC);
        int rootPathLen = 1;
        ssl.generateRootCaCert(rootKey, rootCert, rootSubject, notBefore, notAfter, rootPathLen);

        X509Certificate rootX509 = loadCertificate(rootCert);
        assertTrue(selfVerifies(rootX509),
                "Unexpected self-verification");
        assertTrue(rootX509.getIssuerDN().equals(rootX509.getSubjectDN()),
                "Unexpected self-signed cert");
        assertSubject(rootSubject, rootX509);
        assertEquals(rootPathLen, rootX509.getBasicConstraints(),
                "Expected a certificate with CA:" + true + ", but basic constraints = " + rootX509.getBasicConstraints());
        assertEquals(notBefore.toInstant(), rootX509.getNotBefore().toInstant());
        assertEquals(notAfter.toInstant(), rootX509.getNotAfter().toInstant());

        // Generate an intermediate cert
        Subject intermediateSubject = new Subject.Builder().withCommonName("IntermediateCn").withOrganizationName("MyOrganization").build();
        int intermediatePathLen = 1;
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject, intermediateKey, intermediateCert, notBefore, notAfter, intermediatePathLen);

        X509Certificate intermediateX509 = loadCertificate(intermediateCert);
        assertTrue(intermediateX509.getIssuerDN().equals(rootX509.getSubjectDN()),
                "Unexpected intermediate's issued to be root");
        assertSubject(intermediateSubject, intermediateX509);
        assertEquals(intermediatePathLen, intermediateX509.getBasicConstraints(),
                "Expected a certificate with CA:" + true + ", but basic constraints = " + intermediateX509.getBasicConstraints());
        assertEquals(notBefore.toInstant(), intermediateX509.getNotBefore().toInstant());
        assertEquals(notAfter.toInstant(), intermediateX509.getNotAfter().toInstant());

        File leafKey = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        File leafCert = File.createTempFile("crt-", ".crt");

        doGenerateSignedCert(intermediateKey, intermediateCert, intermediateSubject, leafKey, csr, leafCert, null, "123456", sbj);

        // Validate that when the root cert is trusted and the cert chain includes the leaf+intermediate,
        // that the leaf is considered valid by PKIX validation
        X509Certificate leafX509 = loadCertificate(leafCert);
        Set<TrustAnchor> trustAnchors = Set.of(new TrustAnchor(rootX509, null));
        CertPath cp = CertificateFactory.getInstance("X.509").generateCertPath(List.of(leafX509, intermediateX509));

        PKIXParameters pkixp = new PKIXParameters(trustAnchors);
        pkixp.setRevocationEnabled(false);
        pkixp.setDate(new Date(now.plus(90, ChronoUnit.MINUTES).getEpochSecond() * 1000));

        CertPathValidator.getInstance("PKIX").validate(cp, pkixp);

        leafKey.delete();
        csr.delete();
        leafCert.delete();

        rootKey.delete();
        rootCert.delete();
        intermediateKey.delete();
        intermediateCert.delete();
    }

    @Test
    public void testGenerateClientCert() throws Exception {

        Path path = Files.createTempDirectory(OpenSslCertManagerTest.class.getSimpleName());
        path.toFile().deleteOnExit();
        long fileCount = Files.list(path).count();
        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");
        File store = File.createTempFile("store-", ".p12");

        Subject caSbj = new Subject.Builder().withCommonName("CACommonName").withOrganizationName("CAOrganizationName").build();

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();
        File cert = File.createTempFile("crt-", ".crt");

        ssl.generateSelfSignedCert(caKey, caCert, caSbj, 365);
        doGenerateSignedCert(caKey, caCert, caSbj, key, csr, cert, store, "123456", sbj);

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();
        store.delete();

        assertThat(Files.list(path).count(), is(fileCount));
    }

    @Test
    public void testGenerateClientCertWithSubjectAndAltNames() throws Exception {

        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");
        File store = File.createTempFile("store-", ".p12");

        Subject caSbj = new Subject.Builder().withCommonName("CACommonName").withOrganizationName("CAOrganizationName").build();

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        File cert = File.createTempFile("crt-", ".crt");

        ssl.generateSelfSignedCert(caKey, caCert, caSbj, 365);
        doGenerateSignedCert(caKey, caCert, caSbj, key, csr, cert, store, "123456", sbj);

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();
        store.delete();
    }

    private void doGenerateSignedCert(File caKey, File caCert, Subject caSbj, File key, File csr, File cert,
                                      File keyStore, String keyStorePassword, Subject sbj) throws Exception {
        ssl.generateCsr(key, csr, sbj);

        ssl.generateCert(csr, caKey, caCert, cert, sbj, 365);

        if (keyStore != null) {
            ssl.addKeyAndCertToKeyStore(caKey, caCert, "ca", keyStore, keyStorePassword);
        }

        X509Certificate c = loadCertificate(cert);
        assertCaCertificate(c, false);
        Certificate ca = loadCertificate(caCert);

        c.verify(ca.getPublicKey());

        Principal p = c.getSubjectDN();

        assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));

        if (sbj != null && sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
            final Collection<List<?>> snas = c.getSubjectAlternativeNames();
            if (snas != null) {
                for (final List<?> sanItem : snas) {
                    assertThat(sbj.subjectAltNames().containsValue(sanItem.get(1)), is(true));
                }
            } else {
                fail("Missing expected SAN");
            }
        }

        // keystore verification if provided
        if (keyStore != null) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(new FileInputStream(keyStore), keyStorePassword.toCharArray());

            Key storeKey = store.getKey("ca", keyStorePassword.toCharArray());
            StringBuilder sb = new StringBuilder()
                    .append("-----BEGIN PRIVATE KEY-----")
                    .append(Base64.getEncoder().encodeToString(storeKey.getEncoded()))
                    .append("-----END PRIVATE KEY-----");

            assertThat(sb.toString(), is(new String(Files.readAllBytes(caKey.toPath())).replace("\n", "")));

            X509Certificate storeCert = (X509Certificate) store.getCertificate("ca");
            storeCert.verify(storeCert.getPublicKey());
        }
    }

    @Test
    public void testRenewSelfSignedCertWithSubject() throws Exception {
        Subject caSubject = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();
        doRenewSelfSignedCertWithSubject(caSubject);
    }

    public void doRenewSelfSignedCertWithSubject(Subject caSubject) throws Exception {
        // First generate a self-signed cert
        File caKey = File.createTempFile("key-", ".key");
        File originalCert = File.createTempFile("crt-", ".crt");
        File originalStore = File.createTempFile("crt-", ".p12");

        ((Cmd) () -> ssl.generateSelfSignedCert(caKey, originalCert, caSubject, 365)).exec();
        ssl.addCertToTrustStore(originalCert, "ca", originalStore, "123456");

        X509Certificate x509Certificate1 = loadCertificate(originalCert);
        assertTrue(selfVerifies(x509Certificate1),
                "Unexpected self-verification");
        assertTrue(x509Certificate1.getIssuerDN().equals(x509Certificate1.getSubjectDN()),
                "Unexpected self-signedness");
        // subject verification if provided
        if (caSubject != null) {
            assertSubject(caSubject, x509Certificate1);
        }

        // truststore verification if provided
        if (originalStore != null) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(new FileInputStream(originalStore), "123456".toCharArray());
            X509Certificate storeCert = (X509Certificate) store.getCertificate("ca");
            storeCert.verify(storeCert.getPublicKey());
        }

        // generate a client cert
        File clientKey = File.createTempFile("client-", ".key");
        File csr = File.createTempFile("client-", ".csr");
        File clientCert = File.createTempFile("client-", ".crt");
        Subject clientSubject = new Subject.Builder().withCommonName("MyCommonName").withOrganizationName("MyOrganization").build();
        ssl.generateCsr(clientKey, csr, clientSubject);

        ssl.generateCert(csr, caKey, originalCert, clientCert, clientSubject, 365);
        csr.delete();
        originalCert.delete();
        originalStore.delete();

        // Generate a renewed CA certificate
        File newCert = File.createTempFile("crt-", ".crt");
        File newStore = File.createTempFile("crt-", ".p12");
        ssl.renewSelfSignedCert(caKey, newCert, caSubject, 365);
        // TODO should assert that originalCert actually was changed
        ssl.addCertToTrustStore(newCert, "ca", newStore, "123456");

        X509Certificate x509Certificate = loadCertificate(newCert);
        assertCaCertificate(x509Certificate, true);

        // verify the client cert is valid wrt the new cert.
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(clientCert));
        Certificate ca = cf.generateCertificate(new FileInputStream(newCert));

        c.verify(ca.getPublicKey());

        clientKey.delete();
        clientCert.delete();

        caKey.delete();
        newCert.delete();
        newStore.delete();
    }
}
