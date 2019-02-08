/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OpenSslCertManagerTest {

    private static CertificateFactory certFactory;
    private static CertManager ssl;

    @BeforeClass
    public static void before() throws CertificateException {
        Assume.assumeTrue(System.getProperty("os.name").contains("nux"));
        certFactory = CertificateFactory.getInstance("X.509");
        ssl = new OpenSslCertManager();
    }

    @Test
    public void testGenerateSelfSignedCert() throws Exception {
        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");

        testGenerateSelfSignedCert(key, cert, null);

        key.delete();
        cert.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubject() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");

        testGenerateSelfSignedCert(key, cert, sbj);

        key.delete();
        cert.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubjectAndAltNames() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        testGenerateSelfSignedCert(key, cert, sbj);

        key.delete();
        cert.delete();
    }

    private void testGenerateSelfSignedCert(File key, File cert, Subject sbj) throws Exception {
        ssl.generateSelfSignedCert(key, cert, sbj, 365);

        Certificate c = certFactory.generateCertificate(new FileInputStream(cert));

        c.verify(c.getPublicKey());

        // subject verification if provided
        if (sbj != null) {
            if (c instanceof X509Certificate) {
                X509Certificate x509Certificate = (X509Certificate) c;
                Principal p = x509Certificate.getSubjectDN();

                assertEquals(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), p.getName());

                if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                    final Collection<List<?>> sans = x509Certificate.getSubjectAlternativeNames();
                    assertNotNull(sans);
                    assertEquals(sbj.subjectAltNames().size(), sans.size());
                    for (final List<?> sanItem : sans) {
                        assertTrue(sbj.subjectAltNames().containsValue(sanItem.get(1)));
                    }
                }
            } else {
                fail();
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

        Subject caSbj = new Subject();
        caSbj.setCommonName("CACommonName");
        caSbj.setOrganizationName("CAOrganizationName");

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        File cert = File.createTempFile("crt-", ".crt");

        testGenerateSignedCert(caKey, caCert, caSbj, key, csr, cert, sbj);

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();

        assertEquals(Files.list(path).count(), fileCount);
    }

    @Test
    public void testGenerateSignedCertWithSubjectAndAltNames() throws Exception {

        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");

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

        testGenerateSignedCert(caKey, caCert, caSbj, key, csr, cert, sbj);

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();
    }

    private void testGenerateSignedCert(File caKey, File caCert, Subject caSbj, File key, File csr, File cert, Subject sbj) throws Exception {

        ssl.generateSelfSignedCert(caKey, caCert, caSbj, 365);

        ssl.generateCsr(key, csr, sbj);

        ssl.generateCert(csr, caKey, caCert, cert, sbj, 365);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(cert));
        Certificate ca = cf.generateCertificate(new FileInputStream(caCert));

        c.verify(ca.getPublicKey());

        if (c instanceof X509Certificate) {
            X509Certificate x509Certificate = (X509Certificate) c;
            Principal p = x509Certificate.getSubjectDN();

            assertEquals(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), p.getName());

            if (sbj != null && sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                final Collection<List<?>> snas = x509Certificate.getSubjectAlternativeNames();
                if (snas != null) {
                    for (final List<?> sanItem : snas) {
                        assertTrue(sbj.subjectAltNames().containsValue(sanItem.get(1)));
                    }
                } else {
                    fail();
                }
            }
        } else {
            fail();
        }
    }

    @Test
    public void testRenewSelfSignedCertWithSubject() throws Exception {
        // First generate a self-signed cert
        File caKey = File.createTempFile("key-", ".key");
        File originalCert = File.createTempFile("crt-", ".crt");
        Subject caSubject = new Subject();
        caSubject.setCommonName("MyCommonName");
        caSubject.setOrganizationName("MyOrganization");

        testGenerateSelfSignedCert(caKey, originalCert, caSubject);

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
        originalCert.delete();

        // Generate a renewed CA certificate
        File newCert = File.createTempFile("crt-", ".crt");
        ssl.renewSelfSignedCert(caKey, newCert, caSubject, 365);

        // verify the client cert is valid wrt the new cert.
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(clientCert));
        Certificate ca = cf.generateCertificate(new FileInputStream(newCert));

        c.verify(ca.getPublicKey());

        clientKey.delete();
        clientCert.delete();

        caKey.delete();
        newCert.delete();
    }
}
