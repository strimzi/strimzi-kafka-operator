/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CertificateUtilsTest {

    @Test
    @DisplayName("Test cycle of encoding and decoding certificates between PEM and Java X509 implementations")
    public void pemX509Cycle() throws CertificateException {
        // This certificate is used for testing purposes only and is not a real certificate. It is valid until 2118,
        // so it should not cause any issues with the tests.
        String cert = """
            -----BEGIN CERTIFICATE-----
            MIIDhjCCAm6gAwIBAgIJANzx2pPcYgmlMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNV
            BAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQg
            Q29tcGFueSBMdGQxEzARBgNVBAMMCmNsdXN0ZXItY2EwIBcNMTgwODIzMTYxOTU0
            WhgPMjExODA3MzAxNjE5NTRaMFcxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZh
            dWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMM
            CmNsdXN0ZXItY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDbFnJj
            90sKoM35VszJsfwNvO5dshoeFIb2idf7h+l0h3GMv29j+1XtmLJGzxiYy320KFZr
            3IKWbq+DabqdlqEqZm9NZ1Kq9d7mB10zulQce5JwVZ3FqpCmLku2jHCaDXzTKC3T
            /Xp0O9Oe8+42ysSMCTd8p8aZ4vAyJMCKcoyVCGHrUWVba40D7cQNOlhJplSzHZdL
            FYZ13kwzpT5GpDEPhGVmtF8qV918lSxvdpuepyeFdOSYY88FEMMLLrlZG4QCPyES
            4FpcUXMzzvZeLIlZnKNIYbao3Kx+yZv//wjC80/pqdyoZ5+K5hDxjby2+f+2dh0T
            adKRZC2pp+j3/z63AgMBAAGjUzBRMB0GA1UdDgQWBBThuvddCb/5TPSKYNOHkCTL
            VghhRzAfBgNVHSMEGDAWgBThuvddCb/5TPSKYNOHkCTLVghhRzAPBgNVHRMBAf8E
            BTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBA6oTI27dJgbVtyWxQWznKrkznZ9+t
            mQQGbpfl9zEg7/0X7fFb+m84QHro+aNnQ4kTgZ6QBvusIpwfx1F6lQrraVrPr142
            4DqGmY9xReNu/fj+C+8lTI5PA+mE7tMrLpQvKxI+AMttvlz8eo1SITUA+kJEiWZX
            mjvyHXmhic4K8SnnB0gnFzHN4y09wLqRMNCRH+aI+sa9Wu8cqvpTqlelVcYV83zu
            ydx4VZkC+zTzjI418znN/NU2CMpxLZNl0/zCrspID7v34NRmJ1AHFcrn7/XhsSvz
            D0z+vgrfionoRhyWUDh7POlWwdUOWiBDBOFrkgeKNphSC0glYFN+2IW7
            -----END CERTIFICATE-----""";

        X509Certificate x509 = CertificateUtils.x509Certificate(cert.getBytes());
        assertThat(x509.getSubjectX500Principal().getName(), is("CN=cluster-ca,O=Default Company Ltd,L=Default City,C=XX"));

        String pem = CertificateUtils.x509CertificateToPem(x509);
        assertThat(pem, is(cert));

        X509Certificate nextX509 = CertificateUtils.x509Certificate(pem.getBytes());
        assertThat(nextX509.getSubjectX500Principal().getName(), is("CN=cluster-ca,O=Default Company Ltd,L=Default City,C=XX"));
        assertThat(nextX509.getSignature(), is(x509.getSignature()));
    }

    @Test
    @DisplayName("When certIsTrusted is called it correctly identifies whether the end entity certificate is issued by the CA cert")
    public void testCertIsTrusted() throws IOException, CertificateException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File alternateRootKey = createTempFile("key-", ".key");
        File alternateRootCert = createTempFile("crt-", ".crt");
        File key = createTempFile("key-", ".key");
        File csr = createTempFile("csr-", ".csr");
        File cert = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

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

        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509Cert), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509Cert), x509AlternateRootCert));
    }

    @Test
    @DisplayName("When certIsTrusted is called it correctly identifies whether the end entity certificate is valid against a chain with intermediate certificates")
    public void testCertChainWithIntermediateIsTrusted() throws IOException, CertificateException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File intermediateKey1 = createTempFile("key-", ".key");
        File intermediateCert1 = createTempFile("crt-", ".crt");
        File intermediateKey2 = createTempFile("key-", ".key");
        File intermediateCert2 = createTempFile("crt-", ".crt");
        File leafKey = createTempFile("key-", ".key");
        File csr = createTempFile("csr-", ".csr");
        File leafCert = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

        // Generate an intermediate cert
        Subject intermediateSubject1 = new Subject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1);

        // Generate an additional intermediate cert
        Subject intermediateSubject2 = new Subject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1);


        Subject subject = new Subject.Builder()
                .withCommonName("MyCommonName")
                .withOrganizationName("MyOrganization")
                .addDnsName("example1.com")
                .addDnsName("example2.com").build();

        // Generate leaf cert
        ssl.generateCsr(leafKey, csr, subject);
        ssl.generateCert(csr, intermediateKey2, intermediateCert2, leafCert, subject, 1);

        X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
        X509Certificate x509IntermediateCert1 = (X509Certificate) certFactory.generateCertificate(new FileInputStream(intermediateCert1));
        X509Certificate x509IntermediateCert2 = (X509Certificate) certFactory.generateCertificate(new FileInputStream(intermediateCert2));
        X509Certificate x509LeafCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(leafCert));

        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509IntermediateCert1));
        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509IntermediateCert2));

        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2), x509IntermediateCert1));
        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2, x509IntermediateCert1), x509RootCert));

        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert1, x509IntermediateCert2), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509IntermediateCert2, x509IntermediateCert1, x509LeafCert), x509RootCert));
    }

    private File createTempFile(String prefix, String suffix) throws IOException {
        File file = File.createTempFile(prefix, suffix);
        file.deleteOnExit();
        return file;
    }
}