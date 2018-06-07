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

import static org.junit.Assert.assertEquals;
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

        ssl.generateSelfSignedCert(key, cert, 365);

        Certificate c = certFactory.generateCertificate(new FileInputStream(cert));

        try{
            c.verify(c.getPublicKey());
        } catch (Exception e) {
            fail();
        }

        key.delete();
        cert.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubject() throws IOException, CertificateException {

        File key = File.createTempFile("tls", "key");
        File cert = File.createTempFile("tls", "crt");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCA");
        sbj.setOrganizationName("MyOrgCA");

        ssl.generateSelfSignedCert(key, cert, sbj, 365);

        Certificate c = certFactory.generateCertificate(new FileInputStream(cert));

        try{
            c.verify(c.getPublicKey());
        } catch (Exception e) {
            fail();
        }

        if (c instanceof X509Certificate) {
            X509Certificate x509Certificate = (X509Certificate) c;
            Principal p = x509Certificate.getSubjectDN();
            assertEquals(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), p.getName());
        } else {
            fail();
        }

        key.delete();
        cert.delete();
    }

    @Test
    public void testGenerateSignedCert() throws IOException, CertificateException {

        File caKey = File.createTempFile("tls", "key");
        File caCert = File.createTempFile("tls", "crt");

        ssl.generateSelfSignedCert(caKey, caCert, 365);

        File key = File.createTempFile("tls", "key");
        File csr = File.createTempFile("tls", "csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCN");
        sbj.setOrganizationName("MyOrg");

        ssl.generateCsr(key, csr, sbj);

        File cert = File.createTempFile("tls", "crt");
        ssl.generateCert(csr, caKey, caCert, cert, 365);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(cert));
        Certificate ca = cf.generateCertificate(new FileInputStream(caCert));

        try{
            c.verify(ca.getPublicKey());
        } catch (Exception e) {
            fail();
        }

        if (c instanceof X509Certificate) {
            X509Certificate x509Certificate = (X509Certificate) c;
            Principal p = x509Certificate.getSubjectDN();
            assertEquals(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), p.getName());
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
