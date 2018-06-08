/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OpenSslCertManagerTest {

    private static CertificateFactory certFactory;
    private static CertManager ssl;

    @BeforeClass
    public static void before() throws CertificateException {
        certFactory = CertificateFactory.getInstance("X.509");
        ssl = new OpenSslCertManager();
    }

    @Test
    public void testGenerateSelfSignedCert() throws IOException, CertificateException {

        File key = File.createTempFile("tls", "key");
        File cert = File.createTempFile("tls", "crt");

        testGenerateSelfSignedCert(key, cert, null);

        key.delete();
        cert.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubject() throws IOException, CertificateException {

        File key = File.createTempFile("tls", "key");
        File cert = File.createTempFile("tls", "crt");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");

        testGenerateSelfSignedCert(key, cert, sbj);

        key.delete();
        cert.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubjectAndAltNames() throws IOException, CertificateException {

        File key = File.createTempFile("tls", "key");
        File cert = File.createTempFile("tls", "crt");
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

    private void testGenerateSelfSignedCert(File key, File cert, Subject sbj) throws IOException, CertificateException {
        ssl.generateSelfSignedCert(key, cert, sbj, 365);

        Certificate c = certFactory.generateCertificate(new FileInputStream(cert));

        try {
            c.verify(c.getPublicKey());
        } catch (Exception e) {
            fail();
        }

        // subject verification if provided
        if (sbj != null) {
            if (c instanceof X509Certificate) {
                X509Certificate x509Certificate = (X509Certificate) c;
                Principal p = x509Certificate.getSubjectDN();

                assertEquals(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), p.getName());

                if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                    final Collection<List<?>> snas = x509Certificate.getSubjectAlternativeNames();
                    if (snas != null) {
                        assertEquals(sbj.subjectAltNames().size(), snas.size());
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
    }

    @Test
    public void testGenerateSignedCert() throws IOException, CertificateException {

        File caKey = File.createTempFile("tls", "key");
        File caCert = File.createTempFile("tls", "crt");

        ssl.generateSelfSignedCert(caKey, caCert, 365);

        File key = File.createTempFile("tls", "key");
        File csr = File.createTempFile("tls", "csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        ssl.generateCsr(key, csr, sbj);

        File cert = File.createTempFile("tls", "crt");
        ssl.generateCert(csr, caKey, caCert, cert, 365);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(cert));
        Certificate ca = cf.generateCertificate(new FileInputStream(caCert));

        try {
            c.verify(ca.getPublicKey());
        } catch (Exception e) {
            fail();
        }

        if (c instanceof X509Certificate) {
            X509Certificate x509Certificate = (X509Certificate) c;
            Principal p = x509Certificate.getSubjectDN();

            assertEquals(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), p.getName());

            // TODO : check about "copy_extensions" option for enabling the following code
            // subject alternative names are not transferred automatically from a CSR to the final certificate
            // copy_extensions option is involved but it seems it's a risk to enable it
            /*
            final Collection<List<?>> snas = x509Certificate.getSubjectAlternativeNames();
            if (snas != null) {
                for (final List<?> sanItem : snas) {
                    assertTrue(subjectAltNames.containsValue(sanItem.get(1)));
                }
            } else {
                fail();
            }
            */

        } else {
            fail();
        }

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();
    }
}
