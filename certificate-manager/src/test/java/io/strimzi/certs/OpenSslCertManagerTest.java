/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OpenSslCertManagerTest {

    private static CertificateFactory certFactory;
    private static CertManager ssl;

    @BeforeAll
    public static void before() throws CertificateException {
        Assumptions.assumeTrue(System.getProperty("os.name").contains("nux"));
        certFactory = CertificateFactory.getInstance("X.509");
        ssl = new OpenSslCertManager();
    }

    @Test
    public void testGenerateSelfSignedCert() throws Exception {
        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".p12");

        doGenerateSelfSignedCert(key, cert, store, "123456", null);

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubject() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".p12");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");

        doGenerateSelfSignedCert(key, cert, store, "123456", sbj);

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubjectAndAltNames() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".p12");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        doGenerateSelfSignedCert(key, cert, store, "123456", sbj);

        key.delete();
        cert.delete();
        store.delete();
    }

    private void doGenerateSelfSignedCert(File key, File cert, File trustStore, String trustStorePassword, Subject sbj) throws Exception {
        ssl.generateSelfSignedCert(key, cert, sbj, 365);
        ssl.addCertToTrustStore(cert, "ca", trustStore, trustStorePassword);

        X509Certificate x509Certificate = loadCertificate(cert);
        assertCaCertificate(x509Certificate);
        // subject verification if provided
        if (sbj != null) {
            assertSubject(sbj, x509Certificate);
        }

        // truststore verification if provided
        if (trustStore != null) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(new FileInputStream(trustStore), trustStorePassword.toCharArray());
            X509Certificate storeCert = (X509Certificate) store.getCertificate("ca");
            storeCert.verify(storeCert.getPublicKey());
        }
    }

    private X509Certificate loadCertificate(File cert) throws CertificateException, FileNotFoundException {
        Certificate c1 = certFactory.generateCertificate(new FileInputStream(cert));
        assertTrue(c1 instanceof X509Certificate);
        X509Certificate x509Certificate = (X509Certificate) c1;
        return x509Certificate;
    }

    private void assertCaCertificate(X509Certificate x509Certificate) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {
        try {
            x509Certificate.verify(x509Certificate.getPublicKey());
        } catch (Exception e) {
            fail("Expected a self signed cert", e);
        }
        assertTrue(x509Certificate.getBasicConstraints() >= 0,
                "Expected a certificate with CA:true, but basic constraints = " + x509Certificate.getBasicConstraints());
    }

    private void assertSubject(Subject sbj, X509Certificate x509Certificate) throws CertificateParsingException {
        Principal p = x509Certificate.getSubjectDN();

        assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));

        if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
            final Collection<List<?>> sans = x509Certificate.getSubjectAlternativeNames();
            assertThat(sans, is(notNullValue()));
            assertThat(sbj.subjectAltNames().size(), is(sans.size()));
            for (final List<?> sanItem : sans) {
                assertThat(sbj.subjectAltNames().containsValue(sanItem.get(1)), is(true));
            }
        }
    }

    @Test
    public void testGenerateSignedCert() throws Exception {

        Path path = Files.createTempDirectory(OpenSslCertManagerTest.class.getSimpleName());
        path.toFile().deleteOnExit();
        long fileCount = Files.list(path).count();
        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");
        File store = File.createTempFile("store-", ".p12");

        Subject caSbj = new Subject();
        caSbj.setCommonName("CACommonName");
        caSbj.setOrganizationName("CAOrganizationName");

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        File cert = File.createTempFile("crt-", ".crt");

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
    public void testGenerateSignedCertWithSubjectAndAltNames() throws Exception {

        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");
        File store = File.createTempFile("store-", ".p12");

        Subject caSbj = new Subject();
        caSbj.setCommonName("CACommonName");
        caSbj.setOrganizationName("CAOrganizationName");

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        File cert = File.createTempFile("crt-", ".crt");

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

        ssl.generateSelfSignedCert(caKey, caCert, caSbj, 365);

        ssl.generateCsr(key, csr, sbj);

        ssl.generateCert(csr, caKey, caCert, cert, sbj, 365);

        ssl.addKeyAndCertToKeyStore(caKey, caCert, "ca", keyStore, keyStorePassword);

        X509Certificate c = loadCertificate(cert);
        Certificate ca = loadCertificate(caCert);

        c.verify(ca.getPublicKey());

        if (c instanceof X509Certificate) {
            Principal p = c.getSubjectDN();

            assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));

            if (sbj != null && sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                final Collection<List<?>> snas = c.getSubjectAlternativeNames();
                if (snas != null) {
                    for (final List<?> sanItem : snas) {
                        assertThat(sbj.subjectAltNames().containsValue(sanItem.get(1)), is(true));
                    }
                } else {
                    fail();
                }
            }
        } else {
            fail();
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
        Subject caSubject = new Subject();
        caSubject.setCommonName("MyCommonName");
        caSubject.setOrganizationName("MyOrganization");

        doRenewSelfSignedCertWithSubject(caSubject);
    }

    @Test
    public void testRenewSelfSignedCertWithSubjectAndAltNames() throws Exception {
        Subject caSubject = new Subject();
        caSubject.setCommonName("MyCommonName");
        caSubject.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        caSubject.setSubjectAltNames(subjectAltNames);

        doRenewSelfSignedCertWithSubject(caSubject);
    }

    public void doRenewSelfSignedCertWithSubject(Subject caSubject) throws Exception {
        // First generate a self-signed cert
        File caKey = File.createTempFile("key-", ".key");
        File originalCert = File.createTempFile("crt-", ".crt");
        File originalStore = File.createTempFile("crt-", ".p12");


        doGenerateSelfSignedCert(caKey, originalCert, originalStore, "123456", caSubject);

        // generate a client cert
        File clientKey = File.createTempFile("client-", ".key");
        File csr = File.createTempFile("client-", ".csr");
        File clientCert = File.createTempFile("client-", ".crt");
        Subject clientSubject = new Subject();
        clientSubject.setCommonName("MyCommonName");
        clientSubject.setOrganizationName("MyOrganization");
        ssl.generateCsr(clientKey, csr, clientSubject);

        ssl.generateCert(csr, caKey, originalCert, clientCert, clientSubject, 365);
        csr.delete();
        //originalCert.delete();
        originalStore.delete();

        // Generate a renewed CA certificate
        File newCert = originalCert;//File.createTempFile("crt-", ".crt");
        File newStore = File.createTempFile("crt-", ".p12");
        ssl.renewSelfSignedCert(caKey, newCert, caSubject, 365);
        ssl.addCertToTrustStore(newCert, "ca", newStore, "123456");

        X509Certificate x509Certificate = loadCertificate(newCert);
        assertCaCertificate(x509Certificate);

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
